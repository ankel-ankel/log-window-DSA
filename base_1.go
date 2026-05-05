//go:build ignore

package main

import "github.com/keilerkonzept/sliding-topk-tui-demo/program"

func main() {
	program.RunUI(program.NewNaive(), "Naive")
}
