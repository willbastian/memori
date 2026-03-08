#!/usr/bin/env swift

import AppKit
import Foundation

let canvasWidth: CGFloat = 1600
let canvasHeight: CGFloat = 900
let outputPath = CommandLine.arguments.dropFirst().first ?? "assets/title-card.png"

func color(hex: UInt32, alpha: CGFloat = 1.0) -> NSColor {
    let red = CGFloat((hex >> 16) & 0xff) / 255.0
    let green = CGFloat((hex >> 8) & 0xff) / 255.0
    let blue = CGFloat(hex & 0xff) / 255.0
    return NSColor(calibratedRed: red, green: green, blue: blue, alpha: alpha)
}

enum Palette {
    static let paper = color(hex: 0xFFF5E7)
    static let paperDot = color(hex: 0xD7C5A7, alpha: 0.16)
    static let ink = color(hex: 0x242128)
    static let muted = color(hex: 0x5E5A63)
    static let yellow = color(hex: 0xFFC84F)
    static let coral = color(hex: 0xFF7A59)
    static let sky = color(hex: 0x8BD0FF)
    static let mint = color(hex: 0xA9E9BA)
    static let blush = color(hex: 0xFFD8CC)
    static let outline = color(hex: 0x2B2528)
    static let screen = color(hex: 0x2F3A4A)
    static let cream = color(hex: 0xFFFDF8)
}

func topRect(x: CGFloat, y: CGFloat, width: CGFloat, height: CGFloat) -> NSRect {
    NSRect(x: x, y: canvasHeight - y - height, width: width, height: height)
}

func roundedPath(_ rect: NSRect, radius: CGFloat) -> NSBezierPath {
    NSBezierPath(roundedRect: rect, xRadius: radius, yRadius: radius)
}

func avenir(_ size: CGFloat, _ weight: NSFont.Weight = .regular) -> NSFont {
    let name: String
    switch weight {
    case .heavy, .bold:
        name = "Avenir Next Heavy"
    case .semibold, .medium:
        name = "Avenir Next Demi Bold"
    default:
        name = "Avenir Next Regular"
    }
    return NSFont(name: name, size: size) ?? NSFont.systemFont(ofSize: size, weight: weight)
}

func mono(_ size: CGFloat, _ weight: NSFont.Weight = .regular) -> NSFont {
    NSFont(name: "SFNSMono", size: size) ??
        NSFont(name: "Menlo", size: size) ??
        NSFont.monospacedSystemFont(ofSize: size, weight: weight)
}

func drawText(
    _ text: String,
    rect: NSRect,
    font: NSFont,
    color: NSColor,
    alignment: NSTextAlignment = .left
) {
    let paragraph = NSMutableParagraphStyle()
    paragraph.alignment = alignment
    paragraph.lineBreakMode = .byWordWrapping
    let attrs: [NSAttributedString.Key: Any] = [
        .font: font,
        .foregroundColor: color,
        .paragraphStyle: paragraph
    ]
    NSAttributedString(string: text, attributes: attrs)
        .draw(with: rect, options: [.usesLineFragmentOrigin, .usesFontLeading])
}

func fillPath(_ path: NSBezierPath, color: NSColor) {
    color.setFill()
    path.fill()
}

func strokePath(_ path: NSBezierPath, color: NSColor, width: CGFloat) {
    color.setStroke()
    path.lineWidth = width
    path.stroke()
}

func drawStickerRect(_ rect: NSRect, fill: NSColor, radius: CGFloat = 28, outline: NSColor = Palette.outline) {
    let shadowRect = rect.offsetBy(dx: 10, dy: -10)
    let shadow = roundedPath(shadowRect, radius: radius)
    fillPath(shadow, color: outline.withAlphaComponent(0.18))

    let path = roundedPath(rect, radius: radius)
    fillPath(path, color: fill)
    strokePath(path, color: outline, width: 4)
}

func drawBlob(_ circles: [(CGFloat, CGFloat, CGFloat)], color: NSColor) {
    let blob = NSBezierPath()
    for circle in circles {
        let rect = topRect(x: circle.0 - circle.2, y: circle.1 - circle.2, width: circle.2 * 2, height: circle.2 * 2)
        blob.appendOval(in: rect)
    }
    fillPath(blob, color: color)
}

func drawPill(x: CGFloat, y: CGFloat, width: CGFloat, label: String, fill: NSColor, textColor: NSColor) {
    let rect = topRect(x: x, y: y, width: width, height: 48)
    drawStickerRect(rect, fill: fill, radius: 24)
    drawText(label, rect: rect.insetBy(dx: 18, dy: 10), font: avenir(18, .semibold), color: textColor, alignment: .center)
}

func drawRotatedCard(x: CGFloat, y: CGFloat, width: CGFloat, height: CGFloat, angle: CGFloat, fill: NSColor, title: String, lines: [String], accent: NSColor) {
    let rect = topRect(x: x, y: y, width: width, height: height)
    let center = NSPoint(x: rect.midX, y: rect.midY)

    NSGraphicsContext.saveGraphicsState()
    let transform = NSAffineTransform()
    transform.translateX(by: center.x, yBy: center.y)
    transform.rotate(byDegrees: angle)
    transform.translateX(by: -center.x, yBy: -center.y)
    transform.concat()

    drawStickerRect(rect, fill: fill, radius: 24)

    let tab = roundedPath(NSRect(x: rect.minX + 18, y: rect.maxY - 16, width: 88, height: 8), radius: 4)
    fillPath(tab, color: accent)

    drawText(title, rect: NSRect(x: rect.minX + 18, y: rect.maxY - 58, width: rect.width - 36, height: 28), font: mono(18, .medium), color: Palette.ink)
    var baseline = rect.maxY - 90
    for line in lines {
        drawText(line, rect: NSRect(x: rect.minX + 18, y: baseline, width: rect.width - 36, height: 24), font: mono(15, .regular), color: Palette.muted)
        baseline -= 22
    }

    NSGraphicsContext.restoreGraphicsState()
}

func drawSpeechBubble(x: CGFloat, y: CGFloat, width: CGFloat, height: CGFloat, text: String) {
    let rect = topRect(x: x, y: y, width: width, height: height)
    drawStickerRect(rect, fill: Palette.cream, radius: 30)

    let tail = NSBezierPath()
    tail.move(to: NSPoint(x: rect.minX + 78, y: rect.minY + 10))
    tail.line(to: NSPoint(x: rect.minX + 110, y: rect.minY - 26))
    tail.line(to: NSPoint(x: rect.minX + 124, y: rect.minY + 18))
    tail.close()
    fillPath(tail, color: Palette.cream)
    strokePath(tail, color: Palette.outline, width: 4)

    drawText(text, rect: rect.insetBy(dx: 28, dy: 18), font: mono(28, .medium), color: Palette.ink, alignment: .center)
}

func drawMascot() {
    let body = topRect(x: 140, y: 250, width: 360, height: 360)
    drawStickerRect(body, fill: Palette.yellow, radius: 56)

    let divider = NSBezierPath()
    divider.move(to: NSPoint(x: body.minX + 26, y: body.midY - 8))
    divider.line(to: NSPoint(x: body.maxX - 26, y: body.midY - 8))
    strokePath(divider, color: Palette.outline, width: 4)

    let drawerHandle = roundedPath(NSRect(x: body.midX - 34, y: body.minY + 72, width: 68, height: 18), radius: 9)
    fillPath(drawerHandle, color: Palette.outline.withAlphaComponent(0.18))
    strokePath(drawerHandle, color: Palette.outline, width: 4)

    let screen = topRect(x: 198, y: 310, width: 244, height: 130)
    drawStickerRect(screen, fill: Palette.screen, radius: 32)

    let leftEye = NSBezierPath(ovalIn: NSRect(x: screen.minX + 64, y: screen.midY + 12, width: 18, height: 28))
    let rightEye = NSBezierPath(ovalIn: NSRect(x: screen.minX + 146, y: screen.midY + 6, width: 18, height: 24))
    fillPath(leftEye, color: Palette.cream)
    fillPath(rightEye, color: Palette.cream)

    let brow = NSBezierPath()
    brow.move(to: NSPoint(x: screen.minX + 140, y: screen.midY + 44))
    brow.line(to: NSPoint(x: screen.minX + 176, y: screen.midY + 54))
    strokePath(brow, color: Palette.cream, width: 5)

    let smile = NSBezierPath()
    smile.lineWidth = 5
    smile.lineCapStyle = .round
    smile.move(to: NSPoint(x: screen.minX + 78, y: screen.minY + 34))
    smile.curve(
        to: NSPoint(x: screen.minX + 172, y: screen.minY + 48),
        controlPoint1: NSPoint(x: screen.minX + 110, y: screen.minY + 18),
        controlPoint2: NSPoint(x: screen.minX + 138, y: screen.minY + 58)
    )
    Palette.cream.setStroke()
    smile.stroke()

    let leftArm = NSBezierPath()
    leftArm.lineWidth = 14
    leftArm.lineCapStyle = .round
    leftArm.move(to: NSPoint(x: body.minX + 16, y: body.midY + 44))
    leftArm.curve(
        to: NSPoint(x: body.minX - 60, y: body.midY + 86),
        controlPoint1: NSPoint(x: body.minX - 12, y: body.midY + 60),
        controlPoint2: NSPoint(x: body.minX - 46, y: body.midY + 104)
    )
    strokePath(leftArm, color: Palette.outline, width: 14)

    let rightArm = NSBezierPath()
    rightArm.lineWidth = 14
    rightArm.lineCapStyle = .round
    rightArm.move(to: NSPoint(x: body.maxX - 18, y: body.midY + 28))
    rightArm.curve(
        to: NSPoint(x: body.maxX + 64, y: body.midY + 72),
        controlPoint1: NSPoint(x: body.maxX + 18, y: body.midY + 28),
        controlPoint2: NSPoint(x: body.maxX + 52, y: body.midY + 76)
    )
    strokePath(rightArm, color: Palette.outline, width: 14)

    let leftHand = NSBezierPath(ovalIn: NSRect(x: body.minX - 74, y: body.midY + 70, width: 28, height: 28))
    let rightHand = NSBezierPath(ovalIn: NSRect(x: body.maxX + 52, y: body.midY + 58, width: 28, height: 28))
    fillPath(leftHand, color: Palette.outline)
    fillPath(rightHand, color: Palette.outline)

    let leftLeg = NSBezierPath()
    leftLeg.lineWidth = 14
    leftLeg.lineCapStyle = .round
    leftLeg.move(to: NSPoint(x: body.midX - 72, y: body.minY + 6))
    leftLeg.line(to: NSPoint(x: body.midX - 92, y: body.minY - 54))
    strokePath(leftLeg, color: Palette.outline, width: 14)

    let rightLeg = NSBezierPath()
    rightLeg.lineWidth = 14
    rightLeg.lineCapStyle = .round
    rightLeg.move(to: NSPoint(x: body.midX + 54, y: body.minY + 6))
    rightLeg.line(to: NSPoint(x: body.midX + 70, y: body.minY - 54))
    strokePath(rightLeg, color: Palette.outline, width: 14)

    let leftShoe = roundedPath(NSRect(x: body.midX - 132, y: body.minY - 74, width: 84, height: 30), radius: 15)
    let rightShoe = roundedPath(NSRect(x: body.midX + 34, y: body.minY - 74, width: 84, height: 30), radius: 15)
    fillPath(leftShoe, color: Palette.outline)
    fillPath(rightShoe, color: Palette.outline)
}

var seed: UInt64 = 0xC0DE_2026_0308_55AA
func randomUnit() -> CGFloat {
    seed = seed &* 6364136223846793005 &+ 1
    return CGFloat((seed >> 33) & 0xffff) / CGFloat(0xffff)
}

guard
    let bitmap = NSBitmapImageRep(
        bitmapDataPlanes: nil,
        pixelsWide: Int(canvasWidth),
        pixelsHigh: Int(canvasHeight),
        bitsPerSample: 8,
        samplesPerPixel: 4,
        hasAlpha: true,
        isPlanar: false,
        colorSpaceName: .deviceRGB,
        bytesPerRow: 0,
        bitsPerPixel: 0
    ),
    let context = NSGraphicsContext(bitmapImageRep: bitmap)
else {
    fputs("failed to create bitmap context\n", stderr)
    exit(1)
}

NSGraphicsContext.saveGraphicsState()
NSGraphicsContext.current = context

let fullRect = NSRect(x: 0, y: 0, width: canvasWidth, height: canvasHeight)
fillPath(NSBezierPath(rect: fullRect), color: Palette.paper)

for _ in 0..<700 {
    let x = randomUnit() * canvasWidth
    let y = randomUnit() * canvasHeight
    let size = 1 + randomUnit() * 3
    fillPath(NSBezierPath(ovalIn: NSRect(x: x, y: y, width: size, height: size)), color: Palette.paperDot)
}

drawBlob(
    [(270, 340, 190), (214, 248, 110), (360, 258, 126), (290, 460, 126)],
    color: Palette.blush
)
drawBlob(
    [(1150, 190, 170), (1260, 250, 118), (1090, 310, 120)],
    color: color(hex: 0xDFF1FF)
)
drawBlob(
    [(1320, 700, 120), (1220, 742, 74), (1410, 770, 66)],
    color: color(hex: 0xDDF8CF)
)

drawSpeechBubble(x: 94, y: 86, width: 238, height: 96, text: "show the proof.")
drawMascot()

drawRotatedCard(
    x: 74,
    y: 470,
    width: 170,
    height: 116,
    angle: -10,
    fill: color(hex: 0xFFFDF8),
    title: "issue next",
    lines: ["mem-779f0df", "hero image"],
    accent: Palette.coral
)
drawRotatedCard(
    x: 410,
    y: 206,
    width: 174,
    height: 122,
    angle: 12,
    fill: color(hex: 0xE7F7FF),
    title: "packet",
    lines: ["handoff context", "resume steps"],
    accent: Palette.sky
)
drawRotatedCard(
    x: 420,
    y: 486,
    width: 188,
    height: 122,
    angle: 8,
    fill: color(hex: 0xF2FFE8),
    title: "gate verify",
    lines: ["tests green", "art approved"],
    accent: Palette.mint
)

drawText(
    "memori",
    rect: topRect(x: 700, y: 156, width: 760, height: 150),
    font: avenir(130, .heavy),
    color: Palette.ink
)
drawText(
    "A local-first issue tracker for humans, agents, and anyone tired of “trust me, it’s done.”",
    rect: topRect(x: 710, y: 318, width: 700, height: 110),
    font: avenir(31, .medium),
    color: Palette.muted
)
drawText(
    "It writes things down, freezes the close contract, and keeps a packet handy for the next poor soul who has to resume the thread.",
    rect: topRect(x: 710, y: 430, width: 680, height: 120),
    font: avenir(24, .regular),
    color: Palette.ink.withAlphaComponent(0.78)
)

drawPill(x: 710, y: 574, width: 186, label: "append-only", fill: color(hex: 0xFFE3D8), textColor: Palette.coral)
drawPill(x: 914, y: 574, width: 144, label: "gates", fill: color(hex: 0xE9F8FF), textColor: color(hex: 0x2E7BB5))
drawPill(x: 1074, y: 574, width: 162, label: "packets", fill: color(hex: 0xEAFBEA), textColor: color(hex: 0x347A46))

drawStickerRect(topRect(x: 706, y: 664, width: 474, height: 128), fill: Palette.cream, radius: 34)
drawText(
    "$ memori issue next\n$ memori gate verify\n$ memori db replay",
    rect: topRect(x: 738, y: 696, width: 314, height: 92),
    font: mono(23, .medium),
    color: Palette.ink
)
drawText(
    "local-first",
    rect: topRect(x: 1048, y: 704, width: 108, height: 28),
    font: avenir(19, .bold),
    color: Palette.coral,
    alignment: .center
)
drawText(
    "issue tracker",
    rect: topRect(x: 1044, y: 736, width: 122, height: 26),
    font: avenir(17, .medium),
    color: Palette.muted,
    alignment: .center
)

NSGraphicsContext.restoreGraphicsState()

let outputURL = URL(fileURLWithPath: outputPath, relativeTo: URL(fileURLWithPath: FileManager.default.currentDirectoryPath))
try FileManager.default.createDirectory(at: outputURL.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: nil)

guard let data = bitmap.representation(using: .png, properties: [:]) else {
    fputs("failed to encode png\n", stderr)
    exit(1)
}

do {
    try data.write(to: outputURL)
    print(outputURL.path)
} catch {
    fputs("failed to write png: \(error)\n", stderr)
    exit(1)
}
