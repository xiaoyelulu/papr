//! Bounded DAV directory metadata. Never follow hrefs outside this collection.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(super) struct Object {
    pub name: String,
    pub bytes: u64,
    pub etag: String,
    pub modified: i64,
}
pub(super) struct Listing {
    pub objects: BTreeMap<String, Object>,
    pub now: i64,
}
#[derive(Default)]
pub(super) struct Node {
    pub name: String,
    pub text: String,
    pub children: Vec<Node>,
}
impl Node {
    pub fn child(&self, name: &str) -> Option<&Node> {
        self.children.iter().find(|n| n.name == name)
    }
    pub fn text(&self, name: &str) -> Option<&str> {
        self.child(name).map(|n| n.text.trim())
    }
}
pub(super) fn xml(body: &[u8]) -> AppResult<Node> {
    use quick_xml::{events::Event, Reader};
    let mut reader = Reader::from_reader(body);
    let mut stack = vec![Node::default()];
    let mut count = 0;
    loop {
        match reader.read_event().map_err(|_| invalid())? {
            Event::Start(_) | Event::Empty(_) if stack.len() > 32 => return Err(invalid()),
            Event::Start(e) => {
                count += 1;
                if count > 2_000_000 {
                    return Err(invalid());
                }
                stack.push(Node {
                    name: String::from_utf8_lossy(e.local_name().as_ref()).into_owned(),
                    ..Default::default()
                });
            }
            Event::Empty(e) => {
                count += 1;
                if count > 2_000_000 {
                    return Err(invalid());
                }
                stack.last_mut().ok_or_else(invalid)?.children.push(Node {
                    name: String::from_utf8_lossy(e.local_name().as_ref()).into_owned(),
                    ..Default::default()
                });
            }
            Event::Text(e) => stack
                .last_mut()
                .ok_or_else(invalid)?
                .text
                .push_str(&e.unescape().map_err(|_| invalid())?),
            Event::End(_) => {
                if stack.len() < 2 {
                    return Err(invalid());
                }
                let node = stack.pop().unwrap();
                stack.last_mut().unwrap().children.push(node);
            }
            Event::DocType(_) | Event::CData(_) => return Err(invalid()),
            Event::Eof => break,
            _ => {}
        }
    }
    if stack.len() != 1 || stack[0].children.len() != 1 {
        return Err(invalid());
    }
    Ok(stack.pop().unwrap().children.remove(0))
}
pub(super) fn invalid() -> AppError {
    AppError::code("webdavInventoryInvalid")
}
pub(super) fn date(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|d| d.timestamp())
}
pub(super) fn chunk_name(name: &str) -> bool {
    name.strip_prefix("papr-chunk-v6-")
        .and_then(|n| n.strip_suffix(".jsonl.gz"))
        .is_some_and(|h| {
            h.len() == 64
                && h.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        })
}
pub(super) fn session_name(name: &str) -> bool {
    name.strip_prefix("papr-sync-active-")
        .and_then(|n| n.strip_suffix(".json"))
        .is_some_and(|h| {
            h.len() == 32
                && h.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        })
}
pub(super) async fn read_body(mut response: reqwest::Response, limit: usize) -> AppResult<Vec<u8>> {
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err(invalid());
    }
    let mut body = Vec::new();
    while let Some(b) = response.chunk().await.map_err(network_error)? {
        if body.len() + b.len() > limit {
            return Err(invalid());
        }
        body.extend_from_slice(&b);
    }
    Ok(body)
}
pub(super) async fn list(http: &Client, c: &Credentials) -> AppResult<Option<Listing>> {
    let response = transport::retry(&c.url, || async {
        let r = request(http,c,reqwest::Method::from_bytes(b"PROPFIND").unwrap(),c.url.clone())
            .header("Depth","1").header(header::CACHE_CONTROL,"no-cache")
            .header(header::CONTENT_TYPE,"application/xml; charset=utf-8")
            .body("<d:propfind xmlns:d=\"DAV:\"><d:prop><d:getcontentlength/><d:getetag/><d:getlastmodified/><d:resourcetype/></d:prop></d:propfind>").send().await?;
        if matches!(r.status().as_u16(),405|501) { return Ok(r); }
        transport::checked(r).await
    }).await?;
    if matches!(response.status().as_u16(), 405 | 501) {
        transport::drain(response).await;
        return Ok(None);
    }
    if response.status().as_u16() != 207 {
        return Err(invalid());
    }
    let collection = response.url().clone();
    if collection.origin() != c.url.origin() {
        return Err(invalid());
    }
    let now = response
        .headers()
        .get(header::DATE)
        .and_then(|v| v.to_str().ok())
        .and_then(date)
        .ok_or_else(invalid)?;
    let body = read_body(response, 64 * 1024 * 1024).await?;
    let root = xml(&body)?;
    if root.name != "multistatus" {
        return Err(invalid());
    }
    let mut objects = BTreeMap::new();
    for row in &root.children {
        if row.name != "response" {
            continue;
        }
        let Some(href) = row.text("href") else {
            return Err(invalid());
        };
        let url = collection.join(href).map_err(|_| invalid())?;
        let Some(name) = url
            .path()
            .strip_prefix(collection.path())
            .filter(|n| !n.is_empty() && !n.contains('/'))
        else {
            continue;
        };
        if !chunk_name(name) && !session_name(name) {
            continue;
        }
        if url != collection.join(name)? || url.origin() != c.url.origin() {
            return Err(invalid());
        }
        let props = row
            .children
            .iter()
            .filter(|n| {
                n.name == "propstat"
                    && n.text("status")
                        .is_some_and(|s| s.split_whitespace().nth(1) == Some("200"))
            })
            .filter_map(|n| n.child("prop"))
            .collect::<Vec<_>>();
        let text = |key| props.iter().find_map(|n| n.text(key));
        if props.iter().any(|p| {
            p.child("resourcetype")
                .is_some_and(|n| n.child("collection").is_some())
        }) {
            continue;
        }
        // Unknown metadata protects the whole operation rather than hiding an
        // active client or manufacturing a deletable file.
        let bytes = text("getcontentlength")
            .and_then(|v| v.parse().ok())
            .ok_or_else(invalid)?;
        let etag = text("getetag")
            .filter(|e| e.starts_with('"') && e.ends_with('"'))
            .ok_or_else(invalid)?
            .to_owned();
        let modified = text("getlastmodified").and_then(date).ok_or_else(invalid)?;
        let object = Object {
            name: name.to_owned(),
            bytes,
            etag,
            modified,
        };
        if objects.insert(name.to_owned(), object).is_some() || objects.len() > 200_000 {
            return Err(invalid());
        }
    }
    Ok(Some(Listing { objects, now }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_entities_deep_xml_and_unsafe_names() {
        assert!(xml(b"<!DOCTYPE a [<!ENTITY x SYSTEM 'file:///etc/passwd'>]><a>&x;</a>").is_err());
        assert!(xml(("<a>".repeat(40) + &"</a>".repeat(40)).as_bytes()).is_err());
        assert!(!chunk_name("../papr-chunk-v6-x.jsonl.gz"));
        assert!(!session_name("papr-sync-active-unknown.json"));
    }
}
