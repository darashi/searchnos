use std::fmt;
use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub struct ClientAddr {
    socket_addr: SocketAddr,
    forwarded_raw: Option<String>,
}

impl ClientAddr {
    pub fn new(socket_addr: SocketAddr, forwarded_raw: Option<String>) -> Self {
        Self {
            socket_addr,
            forwarded_raw,
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    pub fn forwarded_raw(&self) -> Option<&str> {
        self.forwarded_raw.as_deref()
    }

    pub fn from_headers(socket_addr: SocketAddr, forwarded_header: Option<&str>) -> Self {
        let forwarded_raw = forwarded_header.map(str::to_owned).and_then(|value| {
            if value.trim().is_empty() {
                None
            } else {
                Some(value)
            }
        });
        Self::new(socket_addr, forwarded_raw)
    }
}

impl fmt::Display for ClientAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.forwarded_raw {
            Some(forwarded) => write!(f, "{} (via {})", forwarded, self.socket_addr),
            None => write!(f, "{}", self.socket_addr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClientAddr;
    use std::net::SocketAddr;

    #[test]
    fn fallback_to_socket_addr() {
        let socket_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let client = ClientAddr::from_headers(socket_addr, None);
        assert_eq!(client.forwarded_raw(), None);
        assert_eq!(client.to_string(), "127.0.0.1:8080");
    }

    #[test]
    fn store_forwarded_header_verbatim() {
        let socket_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let header = "for=\"[2001:db8:cafe::17]:443\";proto=https";
        let client = ClientAddr::from_headers(socket_addr, Some(header));
        assert_eq!(client.forwarded_raw(), Some(header));
        assert_eq!(
            client.to_string(),
            "for=\"[2001:db8:cafe::17]:443\";proto=https (via 127.0.0.1:8080)"
        );
    }

    #[test]
    fn ignore_empty_forwarded_header() {
        let socket_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let client = ClientAddr::from_headers(socket_addr, Some("   "));
        assert!(client.forwarded_raw().is_none());
    }
}
