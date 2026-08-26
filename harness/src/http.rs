//! Minimal HTTP/1.1 over `TcpStream`, for talking to ClickHouse and the Schema
//! Registry.
//!
//! Forked from `spate/benchmarks/src/lib.rs` at `6f28a8b8912e`. Deliberately
//! not a dependency on `reqwest` or `hyper`: the driver makes a handful of
//! requests to services on the local bench network, and a full async HTTP client
//! would add a runtime and a large dependency tree to a binary whose whole job is
//! to start containers and poll a row count. Both endpoints speak plain HTTP/1.1
//! and neither needs TLS, redirects, connection pooling or HTTP/2.
//!
//! `Connection: close` on every request, so the response ends at EOF and there is
//! no keep-alive framing to get wrong.

use std::io::{Read, Write};
use std::time::Duration;

/// Decodes an HTTP/1.1 response: splits off the headers, un-chunks when the
/// server used `Transfer-Encoding: chunked`.
///
/// ClickHouse chunks anything it cannot length-prefix, which includes most
/// `SELECT` responses, so this is the common path rather than an edge case.
fn decode(raw: &str) -> String {
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    if raw
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        let mut out = String::new();
        let mut rest = body.as_str();
        while let Some((size_line, tail)) = rest.split_once("\r\n") {
            let size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
            if size == 0 {
                break;
            }
            out.push_str(tail.get(..size).unwrap_or(""));
            // +2 steps over the CRLF that terminates each chunk.
            rest = tail.get(size + 2..).unwrap_or("");
        }
        return out;
    }
    body
}

/// Plain HTTP/1.1 GET. Returns the decoded response body.
///
/// # Errors
///
/// Returns the underlying I/O error if the connection or read fails.
pub fn get(host: &str, port: u16, path: &str) -> std::io::Result<String> {
    let mut stream = std::net::TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    )?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    Ok(decode(&raw))
}

/// Plain HTTP/1.1 POST. Returns the decoded response body.
///
/// # Errors
///
/// Returns the underlying I/O error if the connection or read fails.
pub fn post(host: &str, port: u16, path: &str, body: &str) -> std::io::Result<String> {
    post_typed(host, port, path, None, body)
}

/// Like [`post`] but with an explicit read timeout, for requests whose
/// response is legitimately slow — a gate query over a window that grows with
/// the corpus outlives the default before the server has misbehaved at all.
///
/// # Errors
///
/// Returns the underlying I/O error if the connection or read fails.
pub fn post_slow(
    host: &str,
    port: u16,
    path: &str,
    body: &str,
    read_timeout: Duration,
) -> std::io::Result<String> {
    let mut stream = std::net::TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(read_timeout))?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    Ok(decode(&raw))
}

/// Like [`post`] but with an explicit `Content-Type`.
///
/// ClickHouse ignores the header, which is why the plain form omits it — but a
/// Confluent-compatible Schema Registry does not, and rejects a schema
/// registration posted without `application/vnd.schemaregistry.v1+json`.
///
/// # Errors
///
/// Returns the underlying I/O error if the connection or read fails.
pub fn post_typed(
    host: &str,
    port: u16,
    path: &str,
    content_type: Option<&str>,
    body: &str,
) -> std::io::Result<String> {
    let mut stream = std::net::TcpStream::connect((host, port))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    let ctype = content_type.map_or_else(String::new, |c| format!("Content-Type: {c}\r\n"));
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n{ctype}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    Ok(decode(&raw))
}

#[cfg(test)]
mod tests {
    use super::decode;

    #[test]
    fn splits_headers_from_body() {
        let raw = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n42\n";
        assert_eq!(decode(raw), "42\n");
    }

    #[test]
    fn reassembles_a_chunked_body() {
        // ClickHouse chunks most SELECT responses, so this is the common path.
        let raw = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n42\n\r\n0\r\n\r\n";
        assert_eq!(decode(raw), "42\n");
    }

    #[test]
    fn a_truncated_chunk_yields_nothing_rather_than_panicking() {
        // A read timeout mid-response leaves a chunk header promising more bytes
        // than arrived. Dropping the incomplete chunk is the right call — half a
        // chunk is not data — and the important property is that it degrades
        // instead of indexing out of bounds and killing a 30-hour sweep.
        let raw = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nff\r\nshort";
        assert_eq!(decode(raw), "");
    }

    #[test]
    fn a_complete_chunk_before_a_truncated_one_survives() {
        // The corollary: whatever arrived intact is still returned, so a caller
        // polling a row count sees a stale-but-valid number rather than nothing.
        let raw = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n42\n\r\nff\r\nshort";
        assert_eq!(decode(raw), "42\n");
    }

    #[test]
    fn a_response_without_a_header_terminator_is_empty() {
        assert_eq!(decode("garbage"), "");
    }
}
