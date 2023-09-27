// Выполнять работу до первой ошибки двумя способами, с использованием:
//   - sync.WaitGroup
//   - errgroup.Group

package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const WaitBeforeError = 10 * time.Microsecond

func main() {
	logDelimetr := strings.Repeat("-", 120)
	ctxBackground := context.Background()

	// Блок с sync.WaitGroup
	fmt.Printf("%s\nsync.WaitGroup\n%s\n", logDelimetr, logDelimetr)
	wg := &sync.WaitGroup{}
	ctxCancel, cancel := context.WithCancel(ctxBackground)
	wait_wg := withWaitGroup(wg, ctxCancel, cancel)
	<-wait_wg

	// Блок с errgroup.Group
	fmt.Printf("%s\nerrgroup.Group\n%s\n", logDelimetr, logDelimetr)
	group, ctxGroup := errgroup.WithContext(ctxBackground)
	wait_eg := withErrGroup(group, ctxGroup)
	<-wait_eg

}

func withWaitGroup(wg *sync.WaitGroup, ctx context.Context, cancel context.CancelFunc) <-chan struct{} {
	ch := make(chan struct{})
	// Ожидается ошибка: opening file open file3_не_существующий_файл.csv: no such file or directory
	for _, file := range []string{"file1.csv", "file2.csv", "file3_не_существующий_файл.csv"} {
		file := file
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, err := read(file)
			if err != nil {
				// Дать другим горутинам чуть почитать свои файлы
				time.Sleep(WaitBeforeError)
				fmt.Printf("error reading file %s: %v\n", file, err)
				cancel()
				return
			}
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("file: %s, context cancelled\n", file)
					return
				case line, ok := <-ch:
					if !ok {
						return
					}
					fmt.Printf("file: %s, line: %s\n", file, line)
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

func withErrGroup(g *errgroup.Group, ctx context.Context) <-chan struct{} {
	ch := make(chan struct{})
	// Ожидается ошибка: opening file open file3_не_существующий_файл.csv: no such file or directory
	for _, file := range []string{"file1.csv", "file2.csv", "file3_не_существующий_файл.csv"} {
		file := file
		g.Go(func() error {
			ch, err := read(file)
			if err != nil {
				// Дать другим горутинам чуть почитать свои файлы
				time.Sleep(WaitBeforeError)
				return fmt.Errorf("error reading file %s: %v\n", file, err)
			}
			for {
				select {
				case <-ctx.Done():
					fmt.Printf("file: %s, context cancelled\n", file)
					return nil
				case line, ok := <-ch:
					if !ok {
						return nil
					}
					fmt.Printf("file: %s, line: %s\n", file, line)
				}
			}
		})
	}
	go func() {
		if err := g.Wait(); err != nil {
			fmt.Printf("Error reading files: %v", err)
		}
		close(ch)
	}()
	return ch
}

func read(file string) (<-chan []string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("opening file %w", err)
	}
	ch := make(chan []string)
	go func() {
		cr := csv.NewReader(f)
		for {
			record, err := cr.Read()
			if errors.Is(err, io.EOF) {
				close(ch)
				return
			}
			ch <- record
		}
	}()
	return ch, nil
}
