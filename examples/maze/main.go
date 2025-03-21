package main

import (
	"math/rand"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

type Move int

const (
	left Move = iota
	right
)

const (
	mazeLength  = 10
	movesNumber = 20
)

var maze = strings.Repeat("-", mazeLength)

// Simulate walking through a maze.
func main() {
	source := ext.NewChanSource(moveChan(500*time.Millisecond, movesNumber))
	positionFlow := flow.NewFold(mazeLength/2, move)
	formatFlow := flow.NewMap(format, 1)
	sink := ext.NewStdoutSink()

	source.
		Via(positionFlow).
		Via(formatFlow).
		To(sink)
}

// move calculates the next position given the current position and a Move.
// If the move is invalid (out of bounds), the original position is returned.
func move(m Move, pos int) int {
	switch {
	case m == left && pos > 0:
		return pos - 1
	case m == right && pos < len(maze)-1:
		return pos + 1
	default:
		return pos
	}
}

// format marks the position with an X.
func format(pos int) string {
	return maze[:pos] + "X" + maze[pos+1:]
}

// moveChan creates a channel that emits n random moves at the specified interval.
func moveChan(interval time.Duration, n int) chan any {
	outChan := make(chan any)

	go func() {
		defer close(outChan)
		// generate a sequence of moves
		moves := generateRandomMoves(n)
		for i := 0; i < n; i++ {
			time.Sleep(interval)
			// send the next move
			outChan <- moves[i]
		}
	}()

	return outChan
}

// generateRandomMoves creates a random sequence of moves with length n.
func generateRandomMoves(n int) []Move {
	if n <= 0 {
		return []Move{}
	}

	moves := make([]Move, n)
	for i := 0; i < n; i++ {
		moves[i] = Move(rand.Intn(2)) //nolint:gosec
	}

	return moves
}
