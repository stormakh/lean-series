// End-to-end test servers for crates/net (ports 9600-9604):
//
//	9600 HTTPS, client certificate required (prometheus/common testdata certs)
//	9601 plain HTTP, same handlers
//	9602 HTTP forward proxy (absolute-form and CONNECT; CONNECT needs X-Proxy-Secret: s3 and Proxy-Authorization)
//	9603 HTTPS, TLS 1.2 only, no client certificate
//	9604 HTTPS with client cert required, token endpoint for OAuth2 over TLS
package main

import (
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var td = os.Args[1] // prometheus/common config/testdata

var tokenCalls atomic.Int64

func handlers(scheme string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		cn := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			cn = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		fmt.Fprintf(w, "hello %s %s cn=%s ua=%s", scheme, r.Proto, cn, r.UserAgent())
	})
	mux.HandleFunc("/basic", func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "alice" || p != "wonderland" {
			w.WriteHeader(401)
			fmt.Fprintf(w, "bad basic auth %q %q", u, p)
			return
		}
		fmt.Fprint(w, "basic ok")
	})
	mux.HandleFunc("/bearer", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok-123" {
			w.WriteHeader(401)
			fmt.Fprintf(w, "bad bearer %q", r.Header.Get("Authorization"))
			return
		}
		fmt.Fprint(w, "bearer ok")
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		u, p, ok := r.BasicAuth()
		r.ParseForm()
		if !ok || u != "client" || p != "s3cret" || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "read write" || r.Form.Get("extra") != "1" {
			w.WriteHeader(400)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"error":"invalid_client","error_description":"got %s:%s %v"}`, u, p, r.Form)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"oauth-tok-%d","token_type":"bearer","expires_in":3600}`, tokenCalls.Load())
	})
	mux.HandleFunc("/oauth-data", func(w http.ResponseWriter, r *http.Request) {
		a := r.Header.Get("Authorization")
		if !strings.HasPrefix(a, "Bearer oauth-tok-") {
			w.WriteHeader(401)
			fmt.Fprintf(w, "bad oauth %q", a)
			return
		}
		fmt.Fprintf(w, "oauth ok %s calls=%d", a, tokenCalls.Load())
	})
	mux.HandleFunc("/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/hello", http.StatusFound)
	})
	mux.HandleFunc("/redirect-cross", func(w http.ResponseWriter, r *http.Request) {
		// 127.0.0.1 -> localhost: credentials must be dropped
		http.Redirect(w, r, "http://localhost:9601/bearer", http.StatusFound)
	})
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/loop", http.StatusFound)
	})
	mux.HandleFunc("/post307", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/echo", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%s %s %s", r.Method, r.Header.Get("X-Custom"), b)
	})
	mux.HandleFunc("/gzip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		fmt.Fprint(gz, strings.Repeat("metric_a 1\n", 1000))
		gz.Close()
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", 100000)))
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		// drips one byte every 200 ms for 4 s: a per-read timeout alone would never fire
		f := w.(http.Flusher)
		for i := 0; i < 20; i++ {
			w.Write([]byte("x"))
			f.Flush()
			time.Sleep(200 * time.Millisecond)
		}
	})
	mux.HandleFunc("/chunked", func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "part%d;", i)
			f.Flush()
		}
	})
	return mux
}

func tlsConfig(requireClient bool, maxVersion uint16) *tls.Config {
	cert, err := tls.LoadX509KeyPair(td+"/server.crt", td+"/server.key")
	if err != nil {
		log.Fatal(err)
	}
	ca, _ := os.ReadFile(td + "/tls-ca-chain.pem")
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	c := &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, MaxVersion: maxVersion}
	if requireClient {
		c.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return c
}

var proxied atomic.Int64

func proxy(w http.ResponseWriter, r *http.Request) {
	proxied.Add(1)
	pa := r.Header.Get("Proxy-Authorization")
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("puser:ppass"))
	if pa != want {
		w.WriteHeader(http.StatusProxyAuthRequired)
		return
	}
	if r.Method == http.MethodConnect {
		if r.Header.Get("X-Proxy-Secret") != "s3" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		dst, err := net.Dial("tcp", r.Host)
		if err != nil {
			w.WriteHeader(502)
			return
		}
		w.WriteHeader(200)
		hj, _ := w.(http.Hijacker)
		conn, buf, _ := hj.Hijack()
		go func() { io.Copy(dst, buf); dst.Close() }()
		go func() { io.Copy(conn, dst); conn.Close() }()
		return
	}
	r.RequestURI = ""
	r.Header.Del("Proxy-Authorization")
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		w.WriteHeader(502)
		fmt.Fprint(w, err)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.Header().Set("X-Via-Proxy", fmt.Sprint(proxied.Load()))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func main() {
	go func() {
		s := &http.Server{Addr: "127.0.0.1:9600", Handler: handlers("https"), TLSConfig: tlsConfig(true, 0)}
		log.Fatal(s.ListenAndServeTLS("", ""))
	}()
	go func() {
		log.Fatal(http.ListenAndServe("127.0.0.1:9601", handlers("http")))
	}()
	go func() {
		log.Fatal(http.ListenAndServe("127.0.0.1:9602", http.HandlerFunc(proxy)))
	}()
	go func() {
		s := &http.Server{Addr: "127.0.0.1:9603", Handler: handlers("https12"), TLSConfig: tlsConfig(false, tls.VersionTLS12)}
		log.Fatal(s.ListenAndServeTLS("", ""))
	}()
	s := &http.Server{Addr: "127.0.0.1:9604", Handler: handlers("https-token"), TLSConfig: tlsConfig(true, 0)}
	log.Fatal(s.ListenAndServeTLS("", ""))
}
