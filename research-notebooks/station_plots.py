"""Plotting functions shared by the research notebooks and (eventually) the Shiny app.

All functions take pandas DataFrames and matplotlib axes. We avoid using spark, so the
Shiny app can use them with data from a SQL query.

Use in a notebook in the same folder:
    from station_plots import map_background, plot_station_values, add_figure_heading, plot_prediction_maps
"""

import textwrap

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize

NEW_YORK_LATITUDE = 40.73


def map_background(axis, station_locations):
    """Light backdrop for station maps.

    Draws every station location (columns lat and lon) in pale gray, and corrects the
    aspect ratio so New York isn't stretched east-west.
    """
    axis.set_facecolor("#f4f4f4")
    axis.scatter(station_locations.lon, station_locations.lat,
                 s=3, color="#d0d0d0", zorder=1, linewidths=0)
    axis.set_aspect(1 / np.cos(np.radians(NEW_YORK_LATITUDE)))
    axis.set_xlabel("")
    axis.set_ylabel("")
    axis.set_xticks([])
    axis.set_yticks([])


def plot_station_values(axis, stations, value_column, color_scale, color_map, marker_size=8, zorder=2):
    """Plot stations (columns lat and lon) colored by chosen value column.

    Stations are sorted by the value, so the highest values are drawn on top.
    Stations without a location or a value are left out.
    """
    plotted_stations = (stations.dropna(subset=["lat", "lon", value_column])
                        .sort_values(value_column))
    axis.scatter(
        plotted_stations.lon, plotted_stations.lat,
        c=plotted_stations[value_column],
        cmap=color_map, norm=color_scale,
        s=marker_size, zorder=zorder,
    )


def add_figure_heading(figure, title, annotation="", title_size=24, annotation_size=14,
                       annotation_top=0.94, panels_top=0.84, wrap_width=140):
    """Add a bold title at the top left of a figure, and an optional wrapped annotation under it.

    panels_top reserves the space above the panels for the title and the annotation.
    """
    figure.suptitle(title, fontsize=title_size, fontweight="bold", x=0.05, y=0.99, ha="left")
    if annotation:
        figure.text(
            0.05, annotation_top,
            textwrap.fill(" ".join(annotation.split()), width=wrap_width),
            fontsize=annotation_size, ha="left", va="top", linespacing=1.4,
        )
    figure.get_layout_engine().set(rect=(0, 0, 1, panels_top))

# plot_prediction_maps:
# Draws two maps side by side: the bike stockout probability and the "dockout" probability (not sure if that is
# real word but it rhymes with stockout).
# Stations that can get a prediction are colored by their probability.
# Stations with a current stockout are black crosses, stations with a current dockout are empty black circles,
# and stations that are not operating are gray.
def plot_prediction_maps(station_predictions, new_york_prediction_time, station_locations,
                         max_probability_shown=0.5):
    """Two maps side by side: the bike stockout probability and the "dockout" probability.

    station_predictions has one row per station, with the columns of the predictions table.
    Stations that can get a prediction are colored by their probability. Stations with a current
    stockout are black crosses, stations with a current dockout are empty black circles, and
    stations that are not operating are gray.
    Returns the figure, so a notebook can show it and the Shiny app can render it.
    """
    figure, axes = plt.subplots(1, 2, figsize=(16, 10), constrained_layout=True)
    probability_scale = Normalize(0, max_probability_shown)

    titles = {
        "bike": "Bikes: 30 minute stockout probability",
        "dock": "Docks: 30 minute dockout probability",
    }
    already_labels = {"bike": "Current Stockout", "dock": "Current Dockout"}

    # Current stockouts and current dockouts get different markers, so they are easy to tell apart
    current_markers = {
        "bike": dict(marker="x", color="black", s=14),
        "dock": dict(marker="o", facecolors="none", edgecolors="black", s=28, linewidths=1.2),
    }

    for axis, model_type in zip(axes, ["bike", "dock"]):
        map_background(axis, station_locations)
        station_state = station_predictions[f"{model_type}_state"]

        plot_station_values(
            axis, station_predictions[station_state == "eligible"],
            f"p_{model_type}_stockout", probability_scale, "YlOrRd",
            marker_size=14, zorder=3,
        )

        stocked_out_stations = station_predictions[station_state == "stocked_out"]
        axis.scatter(
            stocked_out_stations.lon, stocked_out_stations.lat,
            zorder=4,
            label=f"{already_labels[model_type]} ({len(stocked_out_stations)})",
            **current_markers[model_type],
        )

        inactive_stations = station_predictions[station_state == "unknown"]
        axis.scatter(
            inactive_stations.lon, inactive_stations.lat,
            color="#9e9e9e", s=10, zorder=2,
            label=f"Not operating ({len(inactive_stations)})",
        )

        axis.set_title(titles[model_type], fontsize=16)
        axis.legend(loc="upper left", fontsize=11)

    colorbar = figure.colorbar(
        ScalarMappable(norm=probability_scale, cmap="YlOrRd"),
        ax=axes.tolist(), shrink=.6, extend="max",
    )
    colorbar.set_label("Predicted probability", fontsize=14)

    add_figure_heading(
        figure,
        f"Predictions made at {new_york_prediction_time:%A %B %d, %Y, %I:%M %p} (New York time)",
        title_size=20, panels_top=0.95,
    )
    return figure
