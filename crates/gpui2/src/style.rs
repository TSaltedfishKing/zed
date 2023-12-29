use std::{iter, mem, ops::Range};

use crate::{
    black, phi, point, quad, rems, AbsoluteLength, BorrowWindow, Bounds, ContentMask, Corners,
    CornersRefinement, CursorStyle, DefiniteLength, Edges, EdgesRefinement, Font, FontFeatures,
    FontStyle, FontWeight, Hsla, Length, Pixels, Point, PointRefinement, Rgba, SharedString, Size,
    SizeRefinement, Styled, TextRun, WindowContext,
};
use collections::HashSet;
use refineable::{Cascade, Refineable};
use smallvec::SmallVec;
pub use taffy::style::{
    AlignContent, AlignItems, AlignSelf, Display, FlexDirection, FlexWrap, JustifyContent,
    Overflow, Position,
};

#[cfg(debug_assertions)]
pub struct DebugBelow;

pub type StyleCascade = Cascade<Style>;

#[derive(Clone, Refineable, Debug)]
#[refineable(Debug)]
pub struct Style {
    /// What layout strategy should be used?
    pub display: Display,

    /// Should the element be painted on screen?
    pub visibility: Visibility,

    // Overflow properties
    /// How children overflowing their container should affect layout
    #[refineable]
    pub overflow: Point<Overflow>,
    /// How much space (in points) should be reserved for the scrollbars of `Overflow::Scroll` and `Overflow::Auto` nodes.
    pub scrollbar_width: f32,

    // Position properties
    /// What should the `position` value of this struct use as a base offset?
    pub position: Position,
    /// How should the position of this element be tweaked relative to the layout defined?
    #[refineable]
    pub inset: Edges<Length>,

    // Size properies
    /// Sets the initial size of the item
    #[refineable]
    pub size: Size<Length>,
    /// Controls the minimum size of the item
    #[refineable]
    pub min_size: Size<Length>,
    /// Controls the maximum size of the item
    #[refineable]
    pub max_size: Size<Length>,
    /// Sets the preferred aspect ratio for the item. The ratio is calculated as width divided by height.
    pub aspect_ratio: Option<f32>,

    // Spacing Properties
    /// How large should the margin be on each side?
    #[refineable]
    pub margin: Edges<Length>,
    /// How large should the padding be on each side?
    #[refineable]
    pub padding: Edges<DefiniteLength>,
    /// How large should the border be on each side?
    #[refineable]
    pub border_widths: Edges<AbsoluteLength>,

    // Alignment properties
    /// How this node's children aligned in the cross/block axis?
    pub align_items: Option<AlignItems>,
    /// How this node should be aligned in the cross/block axis. Falls back to the parents [`AlignItems`] if not set
    pub align_self: Option<AlignSelf>,
    /// How should content contained within this item be aligned in the cross/block axis
    pub align_content: Option<AlignContent>,
    /// How should contained within this item be aligned in the main/inline axis
    pub justify_content: Option<JustifyContent>,
    /// How large should the gaps between items in a flex container be?
    #[refineable]
    pub gap: Size<DefiniteLength>,

    // Flexbox properies
    /// Which direction does the main axis flow in?
    pub flex_direction: FlexDirection,
    /// Should elements wrap, or stay in a single line?
    pub flex_wrap: FlexWrap,
    /// Sets the initial main axis size of the item
    pub flex_basis: Length,
    /// The relative rate at which this item grows when it is expanding to fill space, 0.0 is the default value, and this value must be positive.
    pub flex_grow: f32,
    /// The relative rate at which this item shrinks when it is contracting to fit into space, 1.0 is the default value, and this value must be positive.
    pub flex_shrink: f32,

    /// The fill color of this element
    pub background: Option<Fill>,

    /// The border color of this element
    pub border_color: Option<Hsla>,

    /// The radius of the corners of this element
    #[refineable]
    pub corner_radii: Corners<AbsoluteLength>,

    /// Box Shadow of the element
    pub box_shadow: SmallVec<[BoxShadow; 2]>,

    /// TEXT
    pub text: TextStyleRefinement,

    /// The mouse cursor style shown when the mouse pointer is over an element.
    pub mouse_cursor: Option<CursorStyle>,

    pub z_index: Option<u8>,

    #[cfg(debug_assertions)]
    pub debug: bool,
    #[cfg(debug_assertions)]
    pub debug_below: bool,
}

impl Styled for StyleRefinement {
    fn style(&mut self) -> &mut StyleRefinement {
        self
    }
}

#[derive(Default, Clone, Copy, Debug, Eq, PartialEq)]
pub enum Visibility {
    #[default]
    Visible,
    Hidden,
}

#[derive(Clone, Debug)]
pub struct BoxShadow {
    pub color: Hsla,
    pub offset: Point<Pixels>,
    pub blur_radius: Pixels,
    pub spread_radius: Pixels,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum WhiteSpace {
    #[default]
    Normal,
    Nowrap,
}

#[derive(Refineable, Clone, Debug)]
#[refineable(Debug)]
pub struct TextStyle {
    pub color: Hsla,
    pub font_family: SharedString,
    pub font_features: FontFeatures,
    pub font_size: AbsoluteLength,
    pub line_height: DefiniteLength,
    pub font_weight: FontWeight,
    pub font_style: FontStyle,
    pub background_color: Option<Hsla>,
    pub underline: Option<UnderlineStyle>,
    pub white_space: WhiteSpace,
}

impl Default for TextStyle {
    fn default() -> Self {
        TextStyle {
            color: black(),
            font_family: "Helvetica".into(), // todo!("Get a font we know exists on the system")
            font_features: FontFeatures::default(),
            font_size: rems(1.).into(),
            line_height: phi(),
            font_weight: FontWeight::default(),
            font_style: FontStyle::default(),
            background_color: None,
            underline: None,
            white_space: WhiteSpace::Normal,
        }
    }
}

impl TextStyle {
    pub fn highlight(mut self, style: impl Into<HighlightStyle>) -> Self {
        let style = style.into();
        if let Some(weight) = style.font_weight {
            self.font_weight = weight;
        }
        if let Some(style) = style.font_style {
            self.font_style = style;
        }

        if let Some(color) = style.color {
            self.color = self.color.blend(color);
        }

        if let Some(factor) = style.fade_out {
            self.color.fade_out(factor);
        }

        if let Some(background_color) = style.background_color {
            self.background_color = Some(background_color);
        }

        if let Some(underline) = style.underline {
            self.underline = Some(underline);
        }

        self
    }

    pub fn font(&self) -> Font {
        Font {
            family: self.font_family.clone(),
            features: self.font_features.clone(),
            weight: self.font_weight,
            style: self.font_style,
        }
    }

    /// Returns the rounded line height in pixels.
    pub fn line_height_in_pixels(&self, rem_size: Pixels) -> Pixels {
        self.line_height.to_pixels(self.font_size, rem_size).round()
    }

    pub fn to_run(&self, len: usize) -> TextRun {
        TextRun {
            len,
            font: Font {
                family: self.font_family.clone(),
                features: Default::default(),
                weight: self.font_weight,
                style: self.font_style,
            },
            color: self.color,
            background_color: self.background_color,
            underline: self.underline.clone(),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct HighlightStyle {
    pub color: Option<Hsla>,
    pub font_weight: Option<FontWeight>,
    pub font_style: Option<FontStyle>,
    pub background_color: Option<Hsla>,
    pub underline: Option<UnderlineStyle>,
    pub fade_out: Option<f32>,
}

impl Eq for HighlightStyle {}

impl Style {
    pub fn text_style(&self) -> Option<&TextStyleRefinement> {
        if self.text.is_some() {
            Some(&self.text)
        } else {
            None
        }
    }

    pub fn overflow_mask(
        &self,
        bounds: Bounds<Pixels>,
        rem_size: Pixels,
    ) -> Option<ContentMask<Pixels>> {
        match self.overflow {
            Point {
                x: Overflow::Visible,
                y: Overflow::Visible,
            } => None,
            _ => {
                let mut min = bounds.origin;
                let mut max = bounds.lower_right();

                if self
                    .border_color
                    .map_or(false, |color| !color.is_transparent())
                {
                    min.x += self.border_widths.left.to_pixels(rem_size);
                    max.x -= self.border_widths.right.to_pixels(rem_size);
                    min.y += self.border_widths.top.to_pixels(rem_size);
                    max.y -= self.border_widths.bottom.to_pixels(rem_size);
                }

                let bounds = match (
                    self.overflow.x == Overflow::Visible,
                    self.overflow.y == Overflow::Visible,
                ) {
                    // x and y both visible
                    (true, true) => return None,
                    // x visible, y hidden
                    (true, false) => Bounds::from_corners(
                        point(min.x, bounds.origin.y),
                        point(max.x, bounds.lower_right().y),
                    ),
                    // x hidden, y visible
                    (false, true) => Bounds::from_corners(
                        point(bounds.origin.x, min.y),
                        point(bounds.lower_right().x, max.y),
                    ),
                    // both hidden
                    (false, false) => Bounds::from_corners(min, max),
                };

                Some(ContentMask { bounds })
            }
        }
    }

    // pub fn apply_text_style<C, F, R>(&self, cx: &mut C, f: F) -> R
    // where
    //     C: BorrowAppContext,
    //     F: FnOnce(&mut C) -> R,
    // {
    //     if self.text.is_some() {
    //         cx.with_text_style(Some(self.text.clone()), f)
    //     } else {
    //         f(cx)
    //     }
    // }

    // /// Apply overflow to content mask
    // pub fn apply_overflow<C, F, R>(&self, bounds: Bounds<Pixels>, cx: &mut C, f: F) -> R
    // where
    //     C: BorrowWindow,
    //     F: FnOnce(&mut C) -> R,
    // {
    //     let current_mask = cx.content_mask();

    //     let min = current_mask.bounds.origin;
    //     let max = current_mask.bounds.lower_right();

    //     let mask_bounds = match (
    //         self.overflow.x == Overflow::Visible,
    //         self.overflow.y == Overflow::Visible,
    //     ) {
    //         // x and y both visible
    //         (true, true) => return f(cx),
    //         // x visible, y hidden
    //         (true, false) => Bounds::from_corners(
    //             point(min.x, bounds.origin.y),
    //             point(max.x, bounds.lower_right().y),
    //         ),
    //         // x hidden, y visible
    //         (false, true) => Bounds::from_corners(
    //             point(bounds.origin.x, min.y),
    //             point(bounds.lower_right().x, max.y),
    //         ),
    //         // both hidden
    //         (false, false) => bounds,
    //     };
    //     let mask = ContentMask {
    //         bounds: mask_bounds,
    //     };

    //     cx.with_content_mask(Some(mask), f)
    // }

    /// Paints the background of an element styled with this style.
    pub fn paint(
        &self,
        bounds: Bounds<Pixels>,
        cx: &mut WindowContext,
        continuation: impl FnOnce(&mut WindowContext),
    ) {
        #[cfg(debug_assertions)]
        if self.debug_below {
            cx.set_global(DebugBelow)
        }

        #[cfg(debug_assertions)]
        if self.debug || cx.has_global::<DebugBelow>() {
            cx.paint_quad(crate::outline(bounds, crate::red()));
        }

        let rem_size = cx.rem_size();

        cx.with_z_index(0, |cx| {
            cx.paint_shadows(
                bounds,
                self.corner_radii.to_pixels(bounds.size, rem_size),
                &self.box_shadow,
            );
        });

        let background_color = self.background.as_ref().and_then(Fill::color);
        if background_color.map_or(false, |color| !color.is_transparent()) {
            cx.with_z_index(1, |cx| {
                let mut border_color = background_color.unwrap_or_default();
                border_color.a = 0.;
                cx.paint_quad(quad(
                    bounds,
                    self.corner_radii.to_pixels(bounds.size, rem_size),
                    background_color.unwrap_or_default(),
                    Edges::default(),
                    border_color,
                ));
            });
        }

        cx.with_z_index(2, |cx| {
            continuation(cx);
        });

        if self.is_border_visible() {
            cx.with_z_index(3, |cx| {
                let corner_radii = self.corner_radii.to_pixels(bounds.size, rem_size);
                let border_widths = self.border_widths.to_pixels(rem_size);
                let max_border_width = border_widths.max();
                let max_corner_radius = corner_radii.max();

                let top_bounds = Bounds::from_corners(
                    bounds.origin,
                    bounds.upper_right()
                        + point(Pixels::ZERO, max_border_width.max(max_corner_radius)),
                );
                let bottom_bounds = Bounds::from_corners(
                    bounds.lower_left()
                        - point(Pixels::ZERO, max_border_width.max(max_corner_radius)),
                    bounds.lower_right(),
                );
                let left_bounds = Bounds::from_corners(
                    top_bounds.lower_left(),
                    bottom_bounds.origin + point(max_border_width, Pixels::ZERO),
                );
                let right_bounds = Bounds::from_corners(
                    top_bounds.lower_right() - point(max_border_width, Pixels::ZERO),
                    bottom_bounds.upper_right(),
                );

                let mut background = self.border_color.unwrap_or_default();
                background.a = 0.;
                let quad = quad(
                    bounds,
                    corner_radii,
                    background,
                    border_widths,
                    self.border_color.unwrap_or_default(),
                );

                cx.with_content_mask(Some(ContentMask { bounds: top_bounds }), |cx| {
                    cx.paint_quad(quad.clone());
                });
                cx.with_content_mask(
                    Some(ContentMask {
                        bounds: right_bounds,
                    }),
                    |cx| {
                        cx.paint_quad(quad.clone());
                    },
                );
                cx.with_content_mask(
                    Some(ContentMask {
                        bounds: bottom_bounds,
                    }),
                    |cx| {
                        cx.paint_quad(quad.clone());
                    },
                );
                cx.with_content_mask(
                    Some(ContentMask {
                        bounds: left_bounds,
                    }),
                    |cx| {
                        cx.paint_quad(quad);
                    },
                );
            });
        }

        #[cfg(debug_assertions)]
        if self.debug_below {
            cx.remove_global::<DebugBelow>();
        }
    }

    fn is_border_visible(&self) -> bool {
        self.border_color
            .map_or(false, |color| !color.is_transparent())
            && self.border_widths.any(|length| !length.is_zero())
    }
}

impl Default for Style {
    fn default() -> Self {
        Style {
            display: Display::Block,
            visibility: Visibility::Visible,
            overflow: Point {
                x: Overflow::Visible,
                y: Overflow::Visible,
            },
            scrollbar_width: 0.0,
            position: Position::Relative,
            inset: Edges::auto(),
            margin: Edges::<Length>::zero(),
            padding: Edges::<DefiniteLength>::zero(),
            border_widths: Edges::<AbsoluteLength>::zero(),
            size: Size::auto(),
            min_size: Size::auto(),
            max_size: Size::auto(),
            aspect_ratio: None,
            gap: Size::default(),
            // Aligment
            align_items: None,
            align_self: None,
            align_content: None,
            justify_content: None,
            // Flexbox
            flex_direction: FlexDirection::Row,
            flex_wrap: FlexWrap::NoWrap,
            flex_grow: 0.0,
            flex_shrink: 1.0,
            flex_basis: Length::Auto,
            background: None,
            border_color: None,
            corner_radii: Corners::default(),
            box_shadow: Default::default(),
            text: TextStyleRefinement::default(),
            mouse_cursor: None,
            z_index: None,

            #[cfg(debug_assertions)]
            debug: false,
            #[cfg(debug_assertions)]
            debug_below: false,
        }
    }
}

#[derive(Refineable, Copy, Clone, Default, Debug, PartialEq, Eq)]
#[refineable(Debug)]
pub struct UnderlineStyle {
    pub thickness: Pixels,
    pub color: Option<Hsla>,
    pub wavy: bool,
}

#[derive(Clone, Debug)]
pub enum Fill {
    Color(Hsla),
}

impl Fill {
    pub fn color(&self) -> Option<Hsla> {
        match self {
            Fill::Color(color) => Some(*color),
        }
    }
}

impl Default for Fill {
    fn default() -> Self {
        Self::Color(Hsla::default())
    }
}

impl From<Hsla> for Fill {
    fn from(color: Hsla) -> Self {
        Self::Color(color)
    }
}

impl From<Rgba> for Fill {
    fn from(value: Rgba) -> Self {
        Fill::from(Hsla::from(value))
    }
}

impl From<TextStyle> for HighlightStyle {
    fn from(other: TextStyle) -> Self {
        Self::from(&other)
    }
}

impl From<&TextStyle> for HighlightStyle {
    fn from(other: &TextStyle) -> Self {
        Self {
            color: Some(other.color),
            font_weight: Some(other.font_weight),
            font_style: Some(other.font_style),
            background_color: other.background_color,
            underline: other.underline.clone(),
            fade_out: None,
        }
    }
}

impl HighlightStyle {
    pub fn highlight(&mut self, other: HighlightStyle) {
        match (self.color, other.color) {
            (Some(self_color), Some(other_color)) => {
                self.color = Some(Hsla::blend(other_color, self_color));
            }
            (None, Some(other_color)) => {
                self.color = Some(other_color);
            }
            _ => {}
        }

        if other.font_weight.is_some() {
            self.font_weight = other.font_weight;
        }

        if other.font_style.is_some() {
            self.font_style = other.font_style;
        }

        if other.background_color.is_some() {
            self.background_color = other.background_color;
        }

        if other.underline.is_some() {
            self.underline = other.underline;
        }

        match (other.fade_out, self.fade_out) {
            (Some(source_fade), None) => self.fade_out = Some(source_fade),
            (Some(source_fade), Some(dest_fade)) => {
                self.fade_out = Some((dest_fade * (1. + source_fade)).clamp(0., 1.));
            }
            _ => {}
        }
    }
}

impl From<Hsla> for HighlightStyle {
    fn from(color: Hsla) -> Self {
        Self {
            color: Some(color),
            ..Default::default()
        }
    }
}

impl From<FontWeight> for HighlightStyle {
    fn from(font_weight: FontWeight) -> Self {
        Self {
            font_weight: Some(font_weight),
            ..Default::default()
        }
    }
}

impl From<FontStyle> for HighlightStyle {
    fn from(font_style: FontStyle) -> Self {
        Self {
            font_style: Some(font_style),
            ..Default::default()
        }
    }
}

impl From<Rgba> for HighlightStyle {
    fn from(color: Rgba) -> Self {
        Self {
            color: Some(color.into()),
            ..Default::default()
        }
    }
}

pub fn combine_highlights(
    a: impl IntoIterator<Item = (Range<usize>, HighlightStyle)>,
    b: impl IntoIterator<Item = (Range<usize>, HighlightStyle)>,
) -> impl Iterator<Item = (Range<usize>, HighlightStyle)> {
    let mut endpoints = Vec::new();
    let mut highlights = Vec::new();
    for (range, highlight) in a.into_iter().chain(b) {
        if !range.is_empty() {
            let highlight_id = highlights.len();
            endpoints.push((range.start, highlight_id, true));
            endpoints.push((range.end, highlight_id, false));
            highlights.push(highlight);
        }
    }
    endpoints.sort_unstable_by_key(|(position, _, _)| *position);
    let mut endpoints = endpoints.into_iter().peekable();

    let mut active_styles = HashSet::default();
    let mut ix = 0;
    iter::from_fn(move || {
        while let Some((endpoint_ix, highlight_id, is_start)) = endpoints.peek() {
            let prev_index = mem::replace(&mut ix, *endpoint_ix);
            if ix > prev_index && !active_styles.is_empty() {
                let mut current_style = HighlightStyle::default();
                for highlight_id in &active_styles {
                    current_style.highlight(highlights[*highlight_id]);
                }
                return Some((prev_index..ix, current_style));
            }

            if *is_start {
                active_styles.insert(*highlight_id);
            } else {
                active_styles.remove(highlight_id);
            }
            endpoints.next();
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use crate::{blue, green, red, yellow};

    use super::*;

    #[test]
    fn test_combine_highlights() {
        assert_eq!(
            combine_highlights(
                [
                    (0..5, green().into()),
                    (4..10, FontWeight::BOLD.into()),
                    (15..20, yellow().into()),
                ],
                [
                    (2..6, FontStyle::Italic.into()),
                    (1..3, blue().into()),
                    (21..23, red().into()),
                ]
            )
            .collect::<Vec<_>>(),
            [
                (
                    0..1,
                    HighlightStyle {
                        color: Some(green()),
                        ..Default::default()
                    }
                ),
                (
                    1..2,
                    HighlightStyle {
                        color: Some(green()),
                        ..Default::default()
                    }
                ),
                (
                    2..3,
                    HighlightStyle {
                        color: Some(green()),
                        font_style: Some(FontStyle::Italic),
                        ..Default::default()
                    }
                ),
                (
                    3..4,
                    HighlightStyle {
                        color: Some(green()),
                        font_style: Some(FontStyle::Italic),
                        ..Default::default()
                    }
                ),
                (
                    4..5,
                    HighlightStyle {
                        color: Some(green()),
                        font_weight: Some(FontWeight::BOLD),
                        font_style: Some(FontStyle::Italic),
                        ..Default::default()
                    }
                ),
                (
                    5..6,
                    HighlightStyle {
                        font_weight: Some(FontWeight::BOLD),
                        font_style: Some(FontStyle::Italic),
                        ..Default::default()
                    }
                ),
                (
                    6..10,
                    HighlightStyle {
                        font_weight: Some(FontWeight::BOLD),
                        ..Default::default()
                    }
                ),
                (
                    15..20,
                    HighlightStyle {
                        color: Some(yellow()),
                        ..Default::default()
                    }
                ),
                (
                    21..23,
                    HighlightStyle {
                        color: Some(red()),
                        ..Default::default()
                    }
                )
            ]
        );
    }
}
