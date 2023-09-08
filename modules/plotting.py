import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style='ticks', context='talk', 
        palette='Spectral', font_scale=1.1)
from collections.abc import Iterable
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.metrics import roc_auc_score, roc_curve, \
          f1_score, precision_recall_curve, average_precision_score

def plot_roc_and_pr_curves(y_true, y_pred, 
                           suptitle='', name = ''):
    '''
    Plotting function to visualize the ROC and Precisio-Recall curves
    to evaluate matching performance
    '''
    auc_ = roc_auc_score(y_true, y_pred)
    f1_score_ = roc_auc_score(y_true, y_pred)
    vg_pr_score = average_precision_score(y_true, y_pred)

    fpr, tpr, _ = roc_curve(y_true, y_pred)
    prec_, recall_, _ = precision_recall_curve(y_true, y_pred)
    pr_baseline = y_true.sum() / len(y_true)

    fig, ax = plt.subplots(1, 2, figsize=(14, 6))

    # Curva ROC
    ax[0].plot(fpr, tpr, 
               c = '#00A3A4', 
               linewidth = 4, 
               label = f'{name}: {auc_:.3f} AUC')
    ax[0].plot([0, 1], [0, 1], c = 'gray',
            linestyle = '--', label = 'Aleatorio')
    ax[0].legend()
    ax[0].set(xlabel = '1 - Especificidad (FPR)',
              ylabel = 'Sensibilidad (TPR)')

    # Curva de Precision-Sensibilidad
    ax[1].plot(recall_, prec_, c = '#F48000', 
               linewidth = 4, 
               label = f'{name}: {vg_pr_score:.3f} avgPR')
    ax[1].plot([0, 1], [pr_baseline, pr_baseline], 
               c = 'gray',
               linestyle = '--', label = 'Aleatorio')
    ax[1].legend()
    ax[1].set(xlabel = 'Sensibilidad',
            ylabel = 'Precisión')
    for i, title in enumerate(['Curva ROC', 
                            'Curva Precisión-Sensibilidad']):
        ax[i].set(aspect='equal', ylim=(0,1.02), title=title)
        ax[i].grid(True)
    plt.tight_layout()
    plt.suptitle(suptitle, y = 1.02)
    plt.show()
    
    
def plot_cfn_matrix(y_true, 
                    y_pred, 
                    pred_true_thr = 0.8,
                    suptitle=''):
    y_pred = y_pred >= pred_true_thr
    cf_matrix = confusion_matrix(y_true, y_pred)

    labels_a = np.array(
          [f'{i}\n{j}' 
          for i, j in
            zip(['TN', 'FP', 'FN', 'TP'],
                  cf_matrix.flatten()
              )
          ]).reshape(2,2)

    fig, ax = plt.subplots(1, 2, figsize=(12, 4))
    # Valores crudos
    sns.heatmap(cf_matrix, 
              cmap = 'Reds', ax = ax[0], 
              fmt = '', annot = labels_a,
              cbar_kws={'label': 'Samples'})
    # Porcentajes
    sns.heatmap(cf_matrix/np.sum(cf_matrix), 
              cmap = 'Blues', ax = ax[1], 
              fmt = '.2%', annot = True,
              cbar_kws={'label': 'Porcentaje'})
    for i in range(2):
        ax[i].set(aspect = "equal", 
                  xlabel = 'Predichos', ylabel = 'Reales')
        for _, spine in ax[i].spines.items():
            spine.set_visible(True)
    plt.suptitle(suptitle, y = 1.0)
    plt.tight_layout()
    plt.show()
    
    
def plot_class_report(y_true, 
                      y_pred, 
                      pred_true_thr = 0.8,
                      suptitle=''):
    y_class = y_pred >= pred_true_thr
    clf_report = classification_report(y_true, y_class, output_dict=False)
    print(clf_report)
    
    # sns.heatmap(pd.DataFrame(clf_report).iloc[:-1, :].T, annot=True, cmap = 'Greens')
    # plt.suptitle(suptitle, y = 1.0)
    # plt.tight_layout()
    # plt.show()