package kafkalog_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestKafkalog(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kafkalog Suite")
}
