// Package scheduler is the cron scheduler: parsing cron expressions,
// deciding which schedules are due, and leader election so idle instances
// don't all hammer the database every tick. Only robfig/cron/v3's parser
// is used from that library - the tick loop, due-schedule scan, and
// leader election are all ours; see Scheduler.Run.
package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// parser accepts the standard 5-field cron format (minute hour dom month
// dow).
var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// NextRun returns the next time expr fires strictly after 'after'.
//
// 'after' is forced to UTC and so is the result: cron expressions here are
// evaluated in UTC only, never local time or a per-schedule timezone (see
// the schedules migration). robfig/cron/v3's Schedule.Next interprets
// calendar fields (hour, day) using whatever Location the time.Time it's
// given carries, so passing anything other than UTC consistently would
// reintroduce exactly the DST ambiguity UTC-only is meant to avoid.
//
// Always computed forward from 'after', never from a schedule's own
// last_run_at or stale next_run_at - see Scheduler.runOne for why that's
// the missed-tick policy (fire once, skip any backlog) rather than
// catching up every interval that elapsed while nothing was running.
func NextRun(expr string, after time.Time) (time.Time, error) {
	sched, err := parser.Parse(expr)
	if err != nil {
		return time.Time{}, fmt.Errorf("scheduler: parse cron expression %q: %w", expr, err)
	}
	return sched.Next(after.UTC()).UTC(), nil
}
