package cli

import (
	"strings"
	"unicode/utf8"
)

func wrapText(value string, width int) []string {
	width = maxInt(width, 10)
	words := strings.Fields(value)
	if len(words) == 0 {
		return nil
	}
	lines := []string{words[0]}
	for _, word := range words[1:] {
		current := lines[len(lines)-1]
		if visualWidth(current)+1+visualWidth(word) <= width {
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
	if visualWidth(value) >= width {
		return trimVisual(value, width)
	}
	return value + strings.Repeat(" ", width-visualWidth(value))
}

func padVisual(value string, width int) string {
	if visualWidth(value) >= width {
		return value
	}
	return value + strings.Repeat(" ", width-visualWidth(value))
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

func visualWidth(value string) int {
	return utf8.RuneCountInString(stripANSI(value))
}

func trimVisual(value string, width int) string {
	if width <= 0 {
		return ""
	}
	raw := stripANSI(value)
	if utf8.RuneCountInString(raw) <= width {
		return raw
	}
	var out strings.Builder
	count := 0
	for _, r := range raw {
		if count >= width {
			break
		}
		out.WriteRune(r)
		count++
	}
	return out.String()
}

func replaceSegment(line string, start int, segment string) string {
	linePrefix, raw, lineSuffix := splitANSIWrapper(line)
	if start >= visualWidth(raw) {
		return line
	}
	prefix := trimVisual(raw, start)
	suffixStart := start + visualWidth(segment)
	if suffixStart > visualWidth(raw) {
		suffixStart = visualWidth(raw)
	}
	suffix := sliceVisual(raw, suffixStart, visualWidth(raw)-suffixStart)
	return wrapWithANSI(linePrefix, prefix, lineSuffix) + segment + wrapWithANSI(linePrefix, suffix, lineSuffix)
}

func sliceVisual(value string, start, width int) string {
	if width <= 0 {
		return ""
	}
	raw := stripANSI(value)
	if start < 0 {
		start = 0
	}
	var out strings.Builder
	index := 0
	count := 0
	for _, r := range raw {
		if index < start {
			index++
			continue
		}
		if count >= width {
			break
		}
		out.WriteRune(r)
		index++
		count++
	}
	return out.String()
}

func splitANSIWrapper(value string) (prefix, raw, suffix string) {
	if !strings.HasPrefix(value, "\x1b[") || !strings.HasSuffix(value, "\x1b[0m") {
		return "", value, ""
	}
	idx := strings.IndexByte(value, 'm')
	if idx < 0 {
		return "", value, ""
	}
	return value[:idx+1], value[idx+1 : len(value)-4], "\x1b[0m"
}

func wrapWithANSI(prefix, raw, suffix string) string {
	if raw == "" {
		return ""
	}
	return prefix + raw + suffix
}
