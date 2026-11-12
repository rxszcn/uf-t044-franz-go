package kgo

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestMergeDescribeTopicPartitionsPage verifies that paginated
// DescribeTopicPartitions responses are converted into the equivalent
// Metadata response topics, that a topic split across pages is stitched
// back together, and that topic IDs survive the conversion (the id2t
// mapping is built from these).
func TestMergeDescribeTopicPartitionsPage(t *testing.T) {
	idA := [16]byte{1}
	idB := [16]byte{2}

	page := func(topic string, id [16]byte, partitions ...int32) *kmsg.DescribeTopicPartitionsResponse {
		resp := kmsg.NewPtrDescribeTopicPartitionsResponse()
		st := kmsg.NewDescribeTopicPartitionsResponseTopic()
		st.Topic = kmsg.StringPtr(topic)
		st.TopicID = id
		for _, p := range partitions {
			sp := kmsg.NewDescribeTopicPartitionsResponseTopicPartition()
			sp.Partition = p
			sp.LeaderID = 7
			sp.LeaderEpoch = 3
			sp.Replicas = []int32{7}
			sp.ISR = []int32{7}
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
		return resp
	}

	merged := &kmsg.MetadataResponse{}
	topicIdx := make(map[string]int)
	// Topic a straddles the page boundary: partitions 0 and 1 on the first
	// page, partition 2 on the second.
	mergeDescribeTopicPartitionsPage(merged, topicIdx, page("a", idA, 0, 1))
	mergeDescribeTopicPartitionsPage(merged, topicIdx, page("a", idA, 2))
	mergeDescribeTopicPartitionsPage(merged, topicIdx, page("b", idB, 0))

	if len(merged.Topics) != 2 {
		t.Fatalf("got %d merged topics, want 2", len(merged.Topics))
	}
	a := merged.Topics[0]
	if *a.Topic != "a" || a.TopicID != idA {
		t.Fatalf("topic a: got name %q id %v, want a %v", *a.Topic, a.TopicID, idA)
	}
	if len(a.Partitions) != 3 {
		t.Fatalf("topic a: got %d partitions, want 3", len(a.Partitions))
	}
	for i, p := range a.Partitions {
		if p.Partition != int32(i) || p.Leader != 7 || p.LeaderEpoch != 3 {
			t.Errorf("topic a partition %d: got partition %d leader %d epoch %d", i, p.Partition, p.Leader, p.LeaderEpoch)
		}
	}
	b := merged.Topics[1]
	if *b.Topic != "b" || b.TopicID != idB || len(b.Partitions) != 1 {
		t.Errorf("topic b: got name %q id %v partitions %d", *b.Topic, b.TopicID, len(b.Partitions))
	}
}
