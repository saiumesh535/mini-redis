package mini_redis

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
)

type Config struct {
	lock     sync.Mutex
	connPool []net.Conn
}

func constructCommand(command ...string) bytes.Buffer {
	var stringCommand bytes.Buffer
	stringCommand.WriteString("*" + strconv.Itoa(len(command)) + "\r\n")
	for _, v := range command {
		stringCommand.WriteString("$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n")
	}
	return stringCommand
}

func (config *Config) getPool() net.Conn {
	for {
		if len(config.connPool) > 0 {
			config.lock.Lock()
			conn := config.connPool[0]
			config.connPool = config.connPool[1:]
			config.lock.Unlock()
			return conn
		}
	}
}

func (config *Config) setPool(conn net.Conn) {
	config.lock.Lock()
	defer config.lock.Unlock()
	config.connPool = append(config.connPool, conn)
}

func (config *Config) Set(key, value string) error {
	conn := config.getPool()
	defer config.setPool(conn)
	command := constructCommand("SET", key, value)
	if _, err := conn.Write(command.Bytes()); err != nil {
		return err
	}
	res := make([]byte, len(value))
	if _, err := conn.Read(res); err != nil {
		return err
	}
	return nil
}

func (config *Config) Get(key string) ([]byte, error) {
	conn := config.getPool()
	defer config.setPool(conn)
	command := constructCommand("GET", key)
	if _, err := conn.Write(command.Bytes()); err != nil {
		return nil, err
	}
	res := make([]byte, 1024)
	if _, err := conn.Read(res); err != nil {
		return nil, err
	}
	return res, nil
}

func (config *Config) Command(commands ...string) error {
	conn := config.getPool()
	defer config.setPool(conn)
	command := constructCommand(commands...)
	if _, err := conn.Write(command.Bytes()); err != nil {
		return err
	}
	res := make([]byte, 1024)
	if _, err := conn.Read(res); err != nil {
		return err
	}
	return nil
}

func Init(poolSize int, host string) (*Config, error) {
	var config Config
	errChan := make(chan error)
	connChan := make(chan net.Conn)

	for i := 0; i < poolSize; i++ {
		go func() {
			conn, err := net.Dial("tcp", host)
			if err != nil {
				errChan <- err
			}
			connChan <- conn
		}()
	}
	for len(config.connPool) < poolSize{
		select {
		case connErr := <-errChan:
			{
				return nil, connErr
			}
		case lol := <-connChan:
			{
				config.connPool = append(config.connPool, lol)
			}
		}
	}
	fmt.Println(len(config.connPool))
	return &config, nil
}
