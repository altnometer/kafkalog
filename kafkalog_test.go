package kafkalog_test

import (
	"errors"
	"os"
	"strings"

	"github.com/altnometer/kafkalog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

/*
Check, calling Send() calls Init if not initialzed
Calling init() sets intialized to true

If getbrokers fails, init returns error, and Send() panics.

checkinitialized() works as exptected.

test get brokers( borrow from kafka package)
*/

var _ = Describe("Kafkalog", func() {
	Describe("NewAsyncProducer", func() {
		var (
			// ap            kafkalog.AsyncProducer
			api                     kafkalog.IAsyncProducer
			brokersEnvVar           string
			brokers                 []string
			GetSaramaProducerBefore func(p *kafkalog.AsyncProducer) error
		)
		BeforeEach(func() {
			brokersEnvVar = "127.0.0.1:9092,127.0.0.1:9092"
			brokers = strings.Split(brokersEnvVar, ",")
			GetSaramaProducerBefore = kafkalog.GetSaramaProducer
			kafkalog.GetSaramaProducer = func(p *kafkalog.AsyncProducer) error {
				// p.prodr = nil
				return nil

			}
		})
		AfterEach(func() {
			kafkalog.GetSaramaProducer = GetSaramaProducerBefore
		})
		JustBeforeEach(func() {
			os.Setenv("KAFKA_BROKERS", brokersEnvVar)
		})
		Context("when KAFKA_BROKERS env var is an empty string", func() {
			BeforeEach(func() {
				brokersEnvVar = ""
			})
			It("panics with the correct error msg", func() {
				defer func() {
					r := recover()
					Expect(r).NotTo(BeNil())
					Expect(r).To(Equal("NO_KAFKA_BROKERS_ARG_IN_ENV"))

				}()
				api = kafkalog.NewAsyncProducer("testingLogger")
			})
		})
		Context("when no KAFKA_BROKERS env var is set", func() {
			JustBeforeEach(func() {
				os.Unsetenv("KAFKA_BROKERS")
			})
			It("panics with the correct error msg", func() {
				defer func() {
					r := recover()
					Expect(r).NotTo(BeNil())
					Expect(r).To(Equal("NO_KAFKA_BROKERS_ARG_IN_ENV"))

				}()
				api = kafkalog.NewAsyncProducer("testingLogger")
			})
		})
		Context("when GetSaramaProducer fails", func() {
			BeforeEach(func() {
				kafkalog.GetSaramaProducer = func(p *kafkalog.AsyncProducer) error {
					// p.prodr = nil
					return errors.New("mock error")
				}
			})
			It("panics with the correct error msg", func() {
				defer func() {
					r := recover()
					Expect(r).NotTo(BeNil())
					Expect(r).To(Equal("mock error"))

				}()
				api = kafkalog.NewAsyncProducer("testingLogger")
			})
		})
	})
})
