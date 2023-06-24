package job

import (
	"context"
	"testing"
	"time"
)

func BenchmarkNewTask(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewTask(func(context.Context) error {
			return nil
		},
			AllowOption{
				RetryCount: Int64(100),
				RetryDelay: Time(30 * time.Millisecond),
				Timeout:    Time(3 * time.Millisecond),
			},
		)
	}
}

func BenchmarkNewMessage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewMessage(mockMessage{
			message: "foo",
		},
			AllowOption{
				RetryCount: Int64(100),
				RetryDelay: Time(30 * time.Millisecond),
				Timeout:    Time(3 * time.Millisecond),
			},
		)
	}
}

func BenchmarkNewOption(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewOptions(
			AllowOption{
				RetryCount: Int64(100),
				RetryDelay: Time(30 * time.Millisecond),
				Timeout:    Time(3 * time.Millisecond),
			},
		)
	}
}
