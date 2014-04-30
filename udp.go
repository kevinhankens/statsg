/*
 * UDP capture
 */
package main

import (
	"fmt"
	"strings"
	"net"
	"os"
	"time"
	"strconv"
)

const FlushIntervalDuration = 10
const SeparatorNsValue = ":"
const SeparatorValueType = "|"

func main() {

	service := ":1200"
	udpAddr, err := net.ResolveUDPAddr("udp4", service)
	checkError(err)

	conn, err := net.ListenUDP("udp", udpAddr)
	checkError(err)

        // thread to capture memory
        messages := make(chan string)

	// Create a goroutine that will flush messages sent to the shared channel.
	go flushData(messages)

	// Listen for UDP messages.
	for {
		handleClient(conn, messages)
	}
}

func flushData(cs chan string) {

	flush_data := ""
	var flush_now int32 = 0
	var flush_time int32 = 0
	for {
		// Track the current time as we only flush on the interval.
                flush_time = int32(time.Now().Unix())

		// Read from the channel
		select {
		case res := <-cs:
			// Expect the format [namespace]:[value]|[type]
			split1 := strings.Split(res, SeparatorNsValue)
			if len(split1) == 1 {
				// If we didn't find the separator, skip.
				continue
			}
			split2 := strings.Split(split1[1], SeparatorValueType)
			if len(split2) == 1 {
				// If we didn't find the separator, skip.
				continue
			}
			// Ensure that we are saving a numeric string.
			if _, err := strconv.Atoi(split2[0]); err != nil {
				if _, err := strconv.ParseFloat(split2[0], 64); err != nil {
					continue
				}
			}
			ts := time.Now().Unix()
			// Append the message to the flush data.
			flush_data = flush_data + strconv.FormatInt(ts, 10) +
				"," + split1[0] +
				"," + split2[0] +
				"," + split2[1] + "\n"
		case <-time.After(time.Second * FlushIntervalDuration):
			// Timeout after the interval has passed.
		}

		// If the interval has passed, flush the messages.
		if (flush_time - flush_now > FlushIntervalDuration && flush_data != "") {
			writeData(flush_data)
                        flush_data = ""
                        flush_now = flush_time
		}
	}
}

func handleClient(conn *net.UDPConn, cs chan string) {

	var buf [512]byte

	len, addr, err := conn.ReadFromUDP(buf[0:])
	if err != nil {
		return
	}

	// At this point we don't validate, just get the message on the channel asap.
        cs <- strings.TrimSpace(string(buf[:len]))

	conn.WriteToUDP([]byte("Received."), addr)
}

func writeData(text string) {
	filename := "output.txt"
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(text); err != nil {
		panic(err)
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error ", err.Error())
		os.Exit(1)
	}
}
