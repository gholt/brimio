package brimutil

type NullIO struct {
}

func (nw *NullIO) Write(v []byte) (int, error) {
	return len(v), nil
}

func (nw *NullIO) Close() error {
	return nil
}
