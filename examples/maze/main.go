package main

import (
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

type Move int

const (
	LEFT Move = iota
	RIGHT
)

// Simulate waling through a maze
var MAZE = "------"
var MOVES = []Move{RIGHT, RIGHT, LEFT, RIGHT, LEFT, LEFT, RIGHT, RIGHT, RIGHT, LEFT}

func move(m Move, pos int) int {
	if m == RIGHT {
		return pos + 1
	}

	return pos - 1
}

func format(pos int) string {
	// Mark the position with an X
	positionInMaze := MAZE[:pos] + "X" + MAZE[pos+1:]
	return positionInMaze
}

// Make a move every interval
func moveChan(interval time.Duration) chan any {
	// Create a sequence of moves
	outChan := make(chan any)

	go func() {
		ticker := time.NewTicker(interval)
		// Start at position 0
		pos := 0
		for _ = range ticker.C {
			// Send the next move
			outChan <- MOVES[pos]
			// Move to the next position
			pos = (pos + 1)
			if pos >= len(MOVES) {
				// Stop
				close(outChan)
				break
			}
		}
	}()

	return outChan
}

func main() {
	source := ext.NewChanSource(moveChan(time.Second))
	positionFlow := flow.NewFold(0, move)
	formatFlow := flow.NewMap(format, 1)
	sink := ext.NewStdoutSink()

	source.
		Via(positionFlow).
		Via(formatFlow).
		To(sink)
}
