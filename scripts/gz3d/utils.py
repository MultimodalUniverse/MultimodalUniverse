# Adapted by mb010 from https://github.com/CKrawczyk/GZ3D_production/blob/master/Working_with_GZ3D.ipynb
import matplotlib
import matplotlib.gridspec as gridspec
import numpy as np
from astropy.wcs import WCS
from astropy.io import fits
import numpy as np
import gzip
import matplotlib.pyplot as plt
import seaborn as sns

def alpha_overlay(C_a, a_a, C_b, a_b=None):
    # Take a base color (C_a), an alpha map (a_a), and a backgroud image (C_b)
    # and overlay them.
    if a_b is None:
        a_b = np.ones(a_a.shape)
    c_a = np.array([a_a.T] * 3).T * C_a
    c_b = np.array([a_b.T] * 3).T * C_b
    c_out = c_a + ((1 - a_a.T) * c_b.T).T
    return c_out

def alpha_maps(maps, colors=None, vmin=0, vmax=15):
    # Take a dict of segmentation maps and base color values
    # and make an alpha-mask overlay image.
    norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    iter_cycle = iter(matplotlib.rcParams['axes.prop_cycle'])
    for idx, key in enumerate(maps.keys()):
        if colors is None:
            c = next(iter_cycle)['color']
        else:
            c = colors[idx]
        base_color = np.array(matplotlib.colors.to_rgb(c))
        norm_map = norm(maps[key])
        if idx == 0:
            background_color = np.ones(3)
        background_color = alpha_overlay(base_color, norm_map, background_color)
    return background_color

def make_alpha_bar(color, vmin=-1, vmax=15):
    # make a matplotlib color bar for a alpha maks of a single color
    # vmin of -1 to make lables line up correctly
    norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    a_a = norm(range(vmin, vmax))
    C_a = np.array(matplotlib.colors.to_rgb(color))
    new_cm = alpha_overlay(C_a, a_a, np.ones(3))
    return matplotlib.colors.ListedColormap(new_cm), norm

def make_alpha_color(count, color, vmin=1, vmax=15):
    # get the alpha-color for a given value
    norm = matplotlib.colors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    return matplotlib.colors.to_rgb(color) + (norm(count), )

def plot_alpha_bar(color, grid, ticks=[]):
    # plot the alpha color bar
    bar, norm = make_alpha_bar(color)
    ax_bar = plt.subplot(grid)
    cb = matplotlib.colorbar.ColorbarBase(ax_bar, cmap=bar, norm=norm, orientation='vertical', ticks=ticks)
    cb.outline.set_linewidth(0)
    return ax_bar, cb

def zero_theta_line(gz3d):
    phi = gz3d.maps.nsa['elpetro_phi']
    map_wcs = WCS(gz3d.maps['spx_ellcoo_elliptical_azimuth'].header, naxis=2)
    # get the center of the image
    # cx, cy = map_wcs.wcs_world2pix(ra, dec, 0)
    r_map = gz3d.maps['spx_ellcoo_elliptical_radius'].value
    cy, cx = map(np.mean, np.nonzero(np.isclose(r_map, 0, atol=1)))
    # get the max radius
    r = np.sqrt(cx**2 + cy**2)
    # get the end of the line
    x = r * np.sin(np.deg2rad(-phi)) + cx
    y = r * np.cos(np.deg2rad(-phi)) + cy
    # world coords
    ra_line, dec_line = map_wcs.wcs_pix2world([cx, x], [cy, y], 0)
    # image coords
    return map_wcs.wcs_world2pix(ra_line, dec_line, 0)

def plot(maps, wcs=None, plot=True, **plot_kwargs):
    """Plot the overlay image with colorbars.

    Args:
        maps (dict): Dict of the masks to overlay.
        wcs (WCS): WCS object for the masks.
    """
    if "class" in maps.keys():
        maps = {maps['class'][idx]: maps['array'][idx] for idx in range(len(maps['class']))}
    # make the subplot grids
    gs = gridspec.GridSpec(1, 2, width_ratios=[0.9, 0.1], wspace=0.01)
    gs_color_bars = gridspec.GridSpecFromSubplotSpec(1, 4, wspace=0, subplot_spec=gs[1])

    # make the alpha overlay image
    # maps = [data.bar_mask, data.spiral_mask, data.star_mask, data.center_mask]
    colors=[f'C{i}' for i in range(len(maps))]
    all_mask = alpha_maps(maps, colors)

    # plot the overlay image
    ax1 = plt.subplot(gs[0], projection=wcs)
    ax1.imshow(all_mask, **plot_kwargs)

    # make a legend
    plt.legend(handles=[
        matplotlib.patches.Patch(color=colors[idx], label=key) for idx, key in enumerate(maps.keys())
    ], ncol=2, loc='lower center', mode='expand')

    # make colorbars
    for idx, key in enumerate(maps.keys()):
        ax, cb = plot_alpha_bar(colors[idx], gs_color_bars[idx])

    ax.tick_params(axis=u'both', which=u'both', length=0)

    tick_labels = np.arange(0, 16)
    tick_locs = tick_labels - 0.5

    cb.set_ticks(tick_locs)
    cb.set_ticklabels(tick_labels)
    cb.set_label('Count')

    if plot:
        plt.show()
    return ax1, gs, gs_color_bars
