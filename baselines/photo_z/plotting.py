import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
import numpy as np

def plot_redshift(y, y_hat, save_plot=False):
    y = y.cpu().numpy()
    y_hat = y_hat.cpu().numpy()

    r2 = r2_score(y, y_hat)
    fig, ax = plt.subplots(1, 2, figsize=(8,4))

    sns.scatterplot(ax=ax[0], x=y, y=y_hat, s=5, color='.15')
    sns.histplot(ax=ax[0], x=y, y=y_hat, bins=50, pthresh=.1, cmap="mako")
    sns.kdeplot(ax=ax[0], x=y, y=y_hat, levels=5, color='k', linewidths=1.5)
    ax[0].text(0.05, 0.95, f'R2: {r2:.3f}', transform=ax[0].transAxes)

    min_val = min(min(y), min(y_hat))
    max_val = max(max(y), max(y_hat))
    ax[0].plot([min_val, max_val], [min_val, max_val], color='black', linestyle='--')

    ax[0].set_xlabel('True Redshift')
    ax[0].set_ylabel('Predicted Redshift')

    bins = np.linspace(min(y), max(y), 10)
    y_binned = np.digitize(y, bins)
    y_avg = [y_hat[y_binned == i].mean() for i in range(1, len(bins))]
    y_std = [y_hat[y_binned == i].std() for i in range(1, len(bins))]

    sns.scatterplot(ax=ax[1], x=y, y=y_hat, s=2, alpha=0.3, color='black')
    sns.lineplot(ax=ax[1], x=bins[:-1], y=y_std, color='r', label='std')
    ax[1].axhline(0, color='grey', linewidth=1.5, alpha=0.5, linestyle='--')

    ax[1].set_xlabel('True Redshift')

    if save_plot:
        plt.savefig('redshift_plot.png')

    plt.show()