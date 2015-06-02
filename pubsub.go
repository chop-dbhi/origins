package origins

// Publisher defines the Subscribe method that returns a receive only channel and
// channel for receiving an error from the publisher if one occurs. Subscribers can
// choose to the error, however it may impact the downstream integrity of the data.
// Consumers must provide a channel that will be closed when the consumer is done
// receiving facts.
type Publisher interface {
	Subscribe(done <-chan struct{}) (pub <-chan *Fact, errch <-chan error)
}
