//! Built-in colour themes for the terminal UI.

use ratatui::style::Color;
use serde::{Deserialize, Serialize};

/// A saved theme choice. The serde names are part of the on-disk config format.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Theme {
    #[default]
    Redis,
    Dracula,
    CatppuccinMocha,
    Nord,
    GruvboxDark,
    TokyoNight,
}

impl Theme {
    pub const ALL: [Self; 6] = [
        Self::Redis,
        Self::Dracula,
        Self::CatppuccinMocha,
        Self::Nord,
        Self::GruvboxDark,
        Self::TokyoNight,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Redis => "Redis",
            Self::Dracula => "Dracula",
            Self::CatppuccinMocha => "Catppuccin Mocha",
            Self::Nord => "Nord",
            Self::GruvboxDark => "Gruvbox Dark",
            Self::TokyoNight => "Tokyo Night",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::Redis => "classic red",
            Self::Dracula => "purple and pink",
            Self::CatppuccinMocha => "soft lavender",
            Self::Nord => "arctic blue",
            Self::GruvboxDark => "warm retro",
            Self::TokyoNight => "deep blue",
        }
    }

    pub fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|theme| *theme == self)
            .unwrap_or(0)
    }

    pub const fn palette(self) -> Palette {
        match self {
            Self::Redis => Palette {
                background: Color::Reset,
                foreground: Color::Reset,
                highlight_foreground: Color::Black,
                accent: rgb(220, 56, 44),
                dim: rgb(130, 130, 140),
                panel: rgb(60, 60, 70),
                info: Color::Cyan,
                success: Color::Green,
                warning: Color::Yellow,
                magenta: Color::Magenta,
                blue: Color::Blue,
                red: Color::LightRed,
            },
            // https://spec.draculatheme.com/
            Self::Dracula => Palette {
                background: rgb(40, 42, 54),
                foreground: rgb(248, 248, 242),
                highlight_foreground: rgb(40, 42, 54),
                accent: rgb(255, 121, 198),
                dim: rgb(98, 114, 164),
                panel: rgb(68, 71, 90),
                info: rgb(139, 233, 253),
                success: rgb(80, 250, 123),
                warning: rgb(241, 250, 140),
                magenta: rgb(189, 147, 249),
                blue: rgb(139, 233, 253),
                red: rgb(255, 85, 85),
            },
            Self::CatppuccinMocha => Palette {
                background: rgb(30, 30, 46),
                foreground: rgb(205, 214, 244),
                highlight_foreground: rgb(30, 30, 46),
                accent: rgb(203, 166, 247),
                dim: rgb(127, 132, 156),
                panel: rgb(69, 71, 90),
                info: rgb(137, 220, 235),
                success: rgb(166, 227, 161),
                warning: rgb(249, 226, 175),
                magenta: rgb(245, 194, 231),
                blue: rgb(137, 180, 250),
                red: rgb(243, 139, 168),
            },
            Self::Nord => Palette {
                background: rgb(46, 52, 64),
                foreground: rgb(236, 239, 244),
                highlight_foreground: rgb(46, 52, 64),
                accent: rgb(136, 192, 208),
                dim: rgb(129, 161, 193),
                panel: rgb(76, 86, 106),
                info: rgb(143, 188, 187),
                success: rgb(163, 190, 140),
                warning: rgb(235, 203, 139),
                magenta: rgb(180, 142, 173),
                blue: rgb(94, 129, 172),
                red: rgb(191, 97, 106),
            },
            Self::GruvboxDark => Palette {
                background: rgb(40, 40, 40),
                foreground: rgb(235, 219, 178),
                highlight_foreground: rgb(40, 40, 40),
                accent: rgb(251, 73, 52),
                dim: rgb(146, 131, 116),
                panel: rgb(80, 73, 69),
                info: rgb(142, 192, 124),
                success: rgb(184, 187, 38),
                warning: rgb(250, 189, 47),
                magenta: rgb(211, 134, 155),
                blue: rgb(131, 165, 152),
                red: rgb(251, 73, 52),
            },
            Self::TokyoNight => Palette {
                background: rgb(26, 27, 38),
                foreground: rgb(192, 202, 245),
                highlight_foreground: rgb(26, 27, 38),
                accent: rgb(187, 154, 247),
                dim: rgb(86, 95, 137),
                panel: rgb(59, 66, 97),
                info: rgb(125, 207, 255),
                success: rgb(158, 206, 106),
                warning: rgb(224, 175, 104),
                magenta: rgb(187, 154, 247),
                blue: rgb(122, 162, 247),
                red: rgb(247, 118, 142),
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Palette {
    pub background: Color,
    pub foreground: Color,
    /// Text drawn over bright accent or error backgrounds.
    pub highlight_foreground: Color,
    pub accent: Color,
    pub dim: Color,
    pub panel: Color,
    pub info: Color,
    pub success: Color,
    pub warning: Color,
    pub magenta: Color,
    pub blue: Color,
    pub red: Color,
}

const fn rgb(red: u8, green: u8, blue: u8) -> Color {
    Color::Rgb(red, green, blue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn theme_names_are_stable_in_json() {
        assert_eq!(
            serde_json::to_string(&Theme::CatppuccinMocha).unwrap(),
            "\"catppuccin_mocha\""
        );
        assert_eq!(
            serde_json::from_str::<Theme>("\"gruvbox_dark\"").unwrap(),
            Theme::GruvboxDark
        );
    }

    #[test]
    fn redis_inherits_both_terminal_colors() {
        let palette = Theme::Redis.palette();
        assert_eq!(palette.background, Color::Reset);
        assert_eq!(palette.foreground, Color::Reset);
    }

    #[test]
    fn dark_themes_use_their_background_for_highlight_text() {
        for theme in Theme::ALL.into_iter().skip(1) {
            let palette = theme.palette();
            assert_eq!(
                palette.highlight_foreground,
                palette.background,
                "{} should use dark text on bright highlights",
                theme.name()
            );
        }
    }
}
