package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════
// Proxy sources — GitHub raw lists + APIs
// ═══════════════════════════════════════════════════════════════════════

var socks5Sources = []string{
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
	"https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
	"https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
	"https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
	"https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
	"https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt",
	"https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
	"https://raw.githubusercontent.com/Zaeem20/FREE_PROXY_LIST/master/socks5.txt",
	"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt",
	"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000&country=all",
	"https://www.proxy-list.download/api/v1/get?type=socks5",
	"https://api.openproxylist.xyz/socks5.txt",
	"https://proxyspace.pro/socks5.txt",
	"https://spys.me/socks.txt",
}

var httpSources = []string{
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
	"https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
	"https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
	"https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt",
	"https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
	"https://raw.githubusercontent.com/Zaeem20/FREE_PROXY_LIST/master/http.txt",
	"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt",
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
	"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all",
	"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks4&timeout=5000&country=all",
	"https://www.proxy-list.download/api/v1/get?type=http",
	"https://www.proxy-list.download/api/v1/get?type=https",
	"https://www.proxy-list.download/api/v1/get?type=socks4",
	"https://api.openproxylist.xyz/http.txt",
	"https://api.openproxylist.xyz/socks4.txt",
	"https://proxyspace.pro/http.txt",
	"https://proxyspace.pro/socks4.txt",
	"https://spys.me/proxy.txt",
}

// ═══════════════════════════════════════════════════════════════════════
// Fetcher
// ═══════════════════════════════════════════════════════════════════════

func fetchSource(url string, timeout time.Duration) ([]string, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return nil, err
	}

	var proxies []string
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.Contains(line, ":") {
			continue
		}
		parts := strings.Fields(line)
		proxy := parts[0]
		host, port, err := net.SplitHostPort(proxy)
		if err != nil || host == "" || port == "" {
			continue
		}
		if net.ParseIP(host) == nil {
			continue
		}
		proxies = append(proxies, proxy)
	}
	return proxies, nil
}

func fetchAll(sources []string, label string) []string {
	var mu sync.Mutex
	var all []string
	var wg sync.WaitGroup

	for _, src := range sources {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			proxies, err := fetchSource(url, 15*time.Second)
			if err != nil {
				fmt.Printf("  [✗] %s\n", shortURL(url))
				return
			}
			mu.Lock()
			all = append(all, proxies...)
			mu.Unlock()
			fmt.Printf("  [✓] %s → %d\n", shortURL(url), len(proxies))
		}(src)
	}
	wg.Wait()
	fmt.Printf("  %s total: %d\n", label, len(all))
	return all
}

func shortURL(u string) string {
	parts := strings.Split(u, "/")
	last := parts[len(parts)-1]
	if len(last) > 55 {
		return last[:55] + "..."
	}
	return last
}

// ═══════════════════════════════════════════════════════════════════════
// Dedup
// ═══════════════════════════════════════════════════════════════════════

func deduplicate(proxies []string) []string {
	seen := make(map[string]bool, len(proxies))
	var unique []string
	for _, p := range proxies {
		if !seen[p] {
			seen[p] = true
			unique = append(unique, p)
		}
	}
	return unique
}

// ═══════════════════════════════════════════════════════════════════════
// Validators
// ═══════════════════════════════════════════════════════════════════════

func validateSOCKS5(proxy string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", proxy, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	// SOCKS5 greeting: version=5, 1 method, no-auth
	if _, err = conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return false
	}
	buf := make([]byte, 2)
	if _, err = io.ReadFull(conn, buf); err != nil {
		return false
	}
	if buf[0] != 0x05 || buf[1] != 0x00 {
		return false
	}

	// CONNECT to httpbin.org:80
	target := "httpbin.org"
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(target))}
	req = append(req, []byte(target)...)
	req = append(req, 0x00, 0x50)
	if _, err = conn.Write(req); err != nil {
		return false
	}
	resp := make([]byte, 10)
	if _, err = io.ReadFull(conn, resp); err != nil {
		return false
	}
	return resp[1] == 0x00
}

func validateHTTP(proxy string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", proxy, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))

	req := "CONNECT httpbin.org:80 HTTP/1.1\r\nHost: httpbin.org:80\r\n\r\n"
	if _, err = conn.Write([]byte(req)); err != nil {
		return false
	}
	reader := bufio.NewReaderSize(conn, 512)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	return strings.Contains(line, " 200 ")
}

func validateAll(proxies []string, proto string, workers int, timeout time.Duration) []string {
	var mu sync.Mutex
	var working []string
	var tested int64
	total := int64(len(proxies))

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Progress
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				done := atomic.LoadInt64(&tested)
				mu.Lock()
				w := len(working)
				mu.Unlock()
				fmt.Printf("\r  [%s] %d/%d (%.0f%%) | alive: %d   ",
					proto, done, total, float64(done)/float64(total)*100, w)
			}
		}
	}()

	for _, proxy := range proxies {
		wg.Add(1)
		sem <- struct{}{}
		go func(p string) {
			defer wg.Done()
			defer func() { <-sem }()

			ok := false
			switch proto {
			case "socks5":
				ok = validateSOCKS5(p, timeout)
			case "http":
				ok = validateHTTP(p, timeout)
				if !ok {
					ok = validateSOCKS5(p, timeout)
				}
			}
			if ok {
				mu.Lock()
				working = append(working, p)
				mu.Unlock()
			}
			atomic.AddInt64(&tested, 1)
		}(proxy)
	}
	wg.Wait()
	cancel()
	fmt.Printf("\r  [%s] %d/%d tested | %d alive                         \n",
		proto, total, total, len(working))
	return working
}

// ═══════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════

func main() {
	start := time.Now()
	workers := 5000
	timeout := 6 * time.Second
	outAll := "proxies.txt"
	outS5 := "socks5.txt"
	outHTTP := "http.txt"
	validate := true

	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-c", "--concurrency":
			if i+1 < len(os.Args) {
				i++
				fmt.Sscanf(os.Args[i], "%d", &workers)
			}
		case "-t", "--timeout":
			if i+1 < len(os.Args) {
				i++
				var s int
				fmt.Sscanf(os.Args[i], "%d", &s)
				timeout = time.Duration(s) * time.Second
			}
		case "-o", "--output":
			if i+1 < len(os.Args) {
				i++
				outAll = os.Args[i]
			}
		case "--no-validate":
			validate = false
		}
	}

	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║  PROXY HARVESTER v1.0 — Scrape + Validate               ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Printf("Workers: %d | Timeout: %v | Validate: %v\n", workers, timeout, validate)
	fmt.Printf("Sources: %d socks5 + %d http/socks4\n\n", len(socks5Sources), len(httpSources))

	// Phase 1: Fetch
	fmt.Println("━━━ PHASE 1: FETCH ━━━")
	fmt.Println("[SOCKS5]")
	s5raw := fetchAll(socks5Sources, "SOCKS5")
	fmt.Println("\n[HTTP/SOCKS4]")
	httpRaw := fetchAll(httpSources, "HTTP")

	// Dedup
	fmt.Println("\n━━━ DEDUP ━━━")
	s5 := deduplicate(s5raw)
	ht := deduplicate(httpRaw)
	s5set := make(map[string]bool, len(s5))
	for _, p := range s5 {
		s5set[p] = true
	}
	var htOnly []string
	for _, p := range ht {
		if !s5set[p] {
			htOnly = append(htOnly, p)
		}
	}
	fmt.Printf("SOCKS5: %d raw → %d unique\n", len(s5raw), len(s5))
	fmt.Printf("HTTP:   %d raw → %d unique\n", len(httpRaw), len(htOnly))
	fmt.Printf("Total:  %d unique proxies\n", len(s5)+len(htOnly))

	// Phase 2: Validate
	var goodS5, goodHTTP []string
	if validate {
		fmt.Println("\n━━━ PHASE 2: VALIDATE ━━━")
		goodS5 = validateAll(s5, "socks5", workers, timeout)
		goodHTTP = validateAll(htOnly, "http", workers, timeout)
	} else {
		fmt.Println("\n━━━ SKIP VALIDATION ━━━")
		goodS5 = s5
		goodHTTP = htOnly
	}

	// Phase 3: Write
	fmt.Println("\n━━━ PHASE 3: OUTPUT ━━━")
	all := append(goodS5, goodHTTP...)
	sort.Strings(all)
	sort.Strings(goodS5)
	sort.Strings(goodHTTP)
	writeLines(outAll, all)
	writeLines(outS5, goodS5)
	writeLines(outHTTP, goodHTTP)
	fmt.Printf("Combined: %s (%d)\n", outAll, len(all))
	fmt.Printf("SOCKS5:   %s (%d)\n", outS5, len(goodS5))
	fmt.Printf("HTTP:     %s (%d)\n", outHTTP, len(goodHTTP))
	fmt.Printf("\n[DONE] %d alive proxies in %v\n", len(all), time.Since(start).Round(time.Second))
}

func writeLines(path string, lines []string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, l := range lines {
		w.WriteString(l + "\n")
	}
	w.Flush()
}
