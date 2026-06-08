package event

import eventcontract "github.com/prismgo/framework/contracts/event"

func init() {
	var _ eventcontract.Event = AppBooting{}
	var _ eventcontract.Dispatcher = (*Dispatcher)(nil)
	var _ eventcontract.Listener = ListenerFunc(nil)
	var _ Listener = ListenerFunc(nil)
	var _ ShouldQueue = eventcontract.ShouldQueue(nil)
	var _ eventcontract.ShouldQueue = ShouldQueue(nil)
}
