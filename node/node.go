package node

type Node struct {
	Name            string // ip:port
	Api             string
	Cores           int
	Memory          int
	MemoryAllocated int
	Disk            int
	DiskAllocated   int
	Role            string
	TaskCount       int
}

func NewNode(name, api, role string) *Node {
	return &Node{
		Name: name,
		Api:  api,
		Role: role,
	}
}
