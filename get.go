/**
 * HTTP Retrieval.
 */
package main

import (
	"fmt"
	"os"
	"strings"
	"strconv"
	"encoding/json"
)

// A DataPoint defines a metric at a point in time.
type DataPoint struct {
	Timestamp int `json:"ts"`
	Namespace string `json:"ns"`
	Datavalue float64 `json:"val"`
	Datatype string `json:"type"`
}

func main() {
	points := readData("output.txt")
	output, err := json.Marshal(points)
	checkError(err)
fmt.Printf("%q\n", points)
fmt.Printf("%q\n", string(output))
}

// Reads all compatible data from the specified file. Expected format is:
// [timestamp],[namespace],[value],[type] @todo verify this format.
func readData (filename string) (points []DataPoint) {
	tail := ""
	f, err := os.Open(filename)
	checkError(err)
	defer f.Close()

	// Read sections of the file until it is consumed.
	buf := make([]byte, 80)
	for {
		n, err := f.Read(buf)
		if err != nil || n == 0 {
			break
		}
		// Since our buffer may include a segment of a metric at the
		// end, we store that in tail. Since we split on \n, we know
		// that it will be the beginning of a line.
		data := tail + string(buf)
		datapoints := strings.Split(data, "\n")
		if len(datapoints) == 1 {
			continue
		}
		for key, value := range datapoints {
			parsed := strings.Split(value, ",")
			if len(parsed) < 4 {
				if len(datapoints) - 1 == key {
					tail = value
				}
				continue
			}
			ts, err1 := strconv.Atoi(parsed[0])
			iv, err2 := strconv.ParseFloat(parsed[2], 64)

			if err1 != nil || err2 !=nil {
				continue
			}

			point := DataPoint{
				Timestamp: ts,
				Namespace: parsed[1],
				Datavalue: iv,
				Datatype: parsed[3],
			}

			points = append(points, point)
		}
	}
	return points
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error ", err.Error())
		os.Exit(1)
	}
}
