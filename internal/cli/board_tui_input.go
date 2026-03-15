package cli

import "bufio"

var boardTUIReadInputs = func(keyCh chan<- boardKeyInput, errCh chan<- error) {
	go readBoardInputs(bufio.NewReader(boardInput()), keyCh, errCh)
}

func readBoardInputs(reader *bufio.Reader, actions chan<- boardKeyInput, errCh chan<- error) {
	for {
		input, err := readBoardInput(reader)
		if err != nil {
			errCh <- err
			return
		}
		if input.action == boardActionNone && input.text == "" && !input.backspace {
			continue
		}
		actions <- input
	}
}

func readBoardInput(reader *bufio.Reader) (boardKeyInput, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return boardKeyInput{}, err
	}
	switch b {
	case '/':
		return boardKeyInput{action: boardActionSearchOpen}, nil
	case 'q':
		return boardKeyInput{action: boardActionQuit}, nil
	case 'j':
		return boardKeyInput{action: boardActionDown}, nil
	case 'k':
		return boardKeyInput{action: boardActionUp}, nil
	case 'h':
		return boardKeyInput{action: boardActionPrevLane}, nil
	case 'l':
		return boardKeyInput{action: boardActionNextLane}, nil
	case 'g':
		return boardKeyInput{action: boardActionTop}, nil
	case 'G':
		return boardKeyInput{action: boardActionBottom}, nil
	case '?':
		return boardKeyInput{action: boardActionToggleHelp}, nil
	case '[':
		return boardKeyInput{action: boardActionParent}, nil
	case ']':
		return boardKeyInput{action: boardActionChild}, nil
	case '{':
		return boardKeyInput{action: boardActionCollapse}, nil
	case '}':
		return boardKeyInput{action: boardActionExpand}, nil
	case 8, 127:
		return boardKeyInput{backspace: true}, nil
	case '\r', '\n', ' ':
		return boardKeyInput{action: boardActionToggleDetail}, nil
	case 27:
		if reader.Buffered() == 0 {
			return boardKeyInput{action: boardActionQuit}, nil
		}
		next, err := reader.ReadByte()
		if err != nil {
			return boardKeyInput{action: boardActionQuit}, nil
		}
		if next != '[' {
			return boardKeyInput{action: boardActionQuit}, nil
		}
		arrow, err := reader.ReadByte()
		if err != nil {
			return boardKeyInput{}, err
		}
		switch arrow {
		case 'A':
			return boardKeyInput{action: boardActionUp}, nil
		case 'B':
			return boardKeyInput{action: boardActionDown}, nil
		case 'C':
			return boardKeyInput{action: boardActionNextLane}, nil
		case 'D':
			return boardKeyInput{action: boardActionPrevLane}, nil
		default:
			return boardKeyInput{}, nil
		}
	default:
		if b >= 32 && b <= 126 {
			return boardKeyInput{text: string(b)}, nil
		}
		return boardKeyInput{}, nil
	}
}
