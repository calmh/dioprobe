package main

import (
	"bytes"
	"cmp"
	"crypto/rand"
	"flag"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ncw/directio"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rwDesc = prometheus.NewDesc("dioprobe_block_duration_seconds", "", []string{"op"}, nil)

func main() {
	path := flag.String("path", cmp.Or(os.Getenv("DIOPROBE_PATH"), "/var/run"), "path to the test directory")
	addr := flag.String("listen", cmp.Or(os.Getenv("DIOPROBE_LISTEN"), ":9172"), "address to listen on")
	flag.Parse()

	prometheus.MustRegister(&dioprobe{*path})
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/healthz", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	slog.Error("http", "error", http.ListenAndServe(*addr, nil))
}

type dioprobe struct {
	dir string
}

func (*dioprobe) Describe(ch chan<- *prometheus.Desc) {
	ch <- rwDesc
}

func (d *dioprobe) Collect(ch chan<- prometheus.Metric) {
	read, write, err := d.measure()
	if err != nil {
		slog.Error("measure", "error", err)
	}
	ch <- prometheus.MustNewConstMetric(rwDesc, prometheus.GaugeValue, read.Seconds(), "read")
	ch <- prometheus.MustNewConstMetric(rwDesc, prometheus.GaugeValue, write.Seconds(), "write")
}

func (d *dioprobe) measure() (rd, wwd time.Duration, err error) {
	file := filepath.Join(d.dir, fmt.Sprintf("file%d.dat", mrand.Int()))
	defer os.Remove(file)

	block := directio.AlignedBlock(directio.BlockSize)
	_, _ = rand.Read(block)

	t0 := time.Now()
	if err := write(file, block); err != nil {
		return 0, 0, err
	}
	t1 := time.Now()
	if err := read(file, block); err != nil {
		return 0, 0, err
	}
	t2 := time.Now()

	return t2.Sub(t1), t1.Sub(t0), nil
}

func write(file string, block []byte) error {
	fd, err := directio.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}
	defer fd.Close()
	n, err := fd.Write(block)
	if err != nil {
		return err
	}
	if n != len(block) {
		return fmt.Errorf("short write: %d", n)
	}
	return fd.Close()
}

func read(file string, data []byte) error {
	fd, err := directio.OpenFile(file, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer fd.Close()
	block := directio.AlignedBlock(directio.BlockSize)
	n, err := fd.Read(block)
	if err != nil {
		return err
	}
	if n != len(block) {
		return fmt.Errorf("short read: %d", n)
	}
	if !bytes.Equal(block, data) {
		return fmt.Errorf("data mismatch")
	}
	return nil
}
