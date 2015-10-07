package main

import "fmt"

type Foo struct {
	Bar int
}

func test(intf interface{}) {
	ptr, ok := intf.(*Foo)
	if ok {
		ptr.Bar = 1
	} else {
		fmt.Println("conversion failed")
	}
}

func main() {
	ptr := &Foo{0}
	fmt.Println(ptr.Bar) // 0
	test(ptr)
	fmt.Println(ptr.Bar) // 1
}
