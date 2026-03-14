package cli

import "strings"

func wrapText(value string, width int) []string {
	width = maxInt(width, 10)
	words := strings.Fields(value)
	if len(words) == 0 {
		return nil
	}
	lines := []string{words[0]}
	for _, word := range words[1:] {
		current := lines[len(lines)-1]
		if len(current)+1+len(word) <= width {
			lines[len(lines)-1] = current + " " + word
			continue
		}
		lines = append(lines, word)
	}
	return lines
}

func padRight(value string, width int) string {
	if width <= 0 {
		return ""
	}
	if len(value) >= width {
		return value[:width]
	}
	return value + strings.Repeat(" ", width-len(value))
}

func padVisual(value string, width int) string {
	raw := stripANSI(value)
	if len(raw) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-len(raw))
}

func stripANSI(value string) string {
	var out strings.Builder
	inEscape := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if inEscape {
			if ch == 'm' {
				inEscape = false
			}
			continue
		}
		if ch == 0x1b {
			inEscape = true
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
}

func replaceSegment(line string, start int, segment string) string {
	raw := stripANSI(line)
	if start >= len(raw) {
		return line
	}
	prefix := raw[:start]
	suffixStart := start + len(stripANSI(segment))
	if suffixStart > len(raw) {
		suffixStart = len(raw)
	}
	return prefix + segment + raw[suffixStart:]
}
