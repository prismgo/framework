package cmd

import (
	"strconv"
	"time"

	"github.com/prismgo/framework/console"
)

// NewSnapshotCommand 创建 horizon:snapshot 命令。
func NewSnapshotCommand(load RuntimeLoader) console.Command {
	return NewRuntimeCommand("horizon:snapshot", "Persist Horizon metrics snapshot", load, runSnapshotCommand)
}

// runSnapshotCommand 持久化队列长度快照和 collector 当前事件派生 metrics。
//
// 输出边界：只显示 snapshot 时间、queue length 数量、bucket 数和 processed/failed/released/poison 总数；
// 不输出 payload、queue.Envelope、poison body 或 worker runtime 明细。
func runSnapshotCommand(ctx console.CommandContext, runtime Runtime) error {
	snapshot, err := runtime.Snapshot(ctx.Context(), time.Now())
	if err != nil {
		return err
	}
	ctx.IO().Info("Snapshot At: " + formatTime(snapshot.CapturedAt))
	ctx.IO().Info("Queue Lengths: " + formatSnapshotCount(snapshot.QueueLengthStatus, snapshot.QueueLengthCount))
	if snapshot.FlushStatus != "" {
		ctx.IO().Info("Flush Status: " + snapshot.FlushStatus)
		ctx.IO().Info("Flush Windows: " + strconv.Itoa(snapshot.FlushWindowCount))
		ctx.IO().Info("Flush Details: " + strconv.Itoa(snapshot.FlushDetailCount))
		ctx.IO().Info("Flush Diagnostics: " + strconv.Itoa(snapshot.FlushDiagnosticCount))
		ctx.IO().Info("Flush Batch Summaries: " + strconv.Itoa(snapshot.FlushBatchSummaryCount))
		ctx.IO().Info("Flush Drops: " + strconv.FormatInt(snapshot.FlushDropCount, 10))
		if snapshot.FlushQuality != "" {
			ctx.IO().Info("Flush Quality: " + snapshot.FlushQuality)
		}
		ctx.IO().Info("Flush Degraded: " + strconv.FormatBool(snapshot.FlushDegraded))
	}
	ctx.IO().Info("Buckets: " + formatSnapshotCount(snapshot.MetricsStatus, snapshot.BucketCount))
	ctx.IO().Info("Processed: " + strconv.FormatInt(snapshot.Totals.Processed, 10))
	ctx.IO().Info("Failed: " + strconv.FormatInt(snapshot.Totals.Failed, 10))
	ctx.IO().Info("Released: " + strconv.FormatInt(snapshot.Totals.Released, 10))
	ctx.IO().Info("Poison Envelopes: " + strconv.FormatInt(snapshot.Totals.PoisonEnvelopes, 10))
	return nil
}

func formatSnapshotCount(status string, count int) string {
	if status == SnapshotStatusSkipped {
		return SnapshotStatusSkipped
	}
	return strconv.Itoa(count)
}
