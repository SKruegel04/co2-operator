import pandas as pd
import matplotlib.pyplot as plt

def normalize_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Bereitet den Datensatz für die kumulative Berechnung der CO2-Emissionen vor.
    """

    # Timestamp Werte zu DateTime Objekten konvertieren
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Spalte für relative Zeit in Minuten hinzufügen
    df['relative_time_minutes'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds() / 60

    # Spalte für relative Zeit in 5 Minuten Intervallen hinzufügen
    df['relative_time_5min'] = (df['relative_time_minutes'] // 5) * 5

    # Entferne Duplikate, behalte jeweils nur den letzten Wert
    df = df.drop_duplicates(subset=['node_name', 'relative_time_5min', 'value_type'], keep='last')

    # Fehlende Werte mit Forward-Fill und Backward-Fill auffüllen
    df['value'] = df['value'].ffill().bfill()

    # Filtere den Datensatz nach POWER und MOER Werten
    power_df = df[df['value_type'] == 'POWER']
    moer_df = df[df['value_type'] == 'MOER']

    # When wir keine POWER-Werte haben, füllen wir sie mit 1 auf
    # Wir gehen davon aus, dass ein Node, der Werte erzeugt, läuft falls nicht anderweitig angegeben
    if power_df.empty:
        power_df = moer_df.copy()
        power_df['value_type'] = 'POWER'
        power_df['value'] = 1
    
    # Führe die beiden DataFrames zusammen und berechne das Produkt der Werte
    df = pd.merge(power_df, moer_df, on=['relative_time_5min'], suffixes=('_power', '_moer'))
    df['total_moer'] = df['value_power'] * df['value_moer']

    # Summiere total_moer
    df = df.groupby(['relative_time_5min'])['total_moer'].sum().reset_index()
    
    # Kumulative Summe der CO2-Emissionen berechnen
    df['cumulative_total_moer'] = df['total_moer'].cumsum()

    return df

def plot(input_name: str, output_name: str, max_minutes: int, time_label: str) -> None:
    """
    Plottet das Produkt aus Power und MOER Werten für alle Knoten über die Zeit.
    """

    # df_with_operator = normalize_data(pd.read_csv('datasets/20240714_node_metric_entries_more_fluct.csv'))
    # df_without_operator = normalize_data(pd.read_csv('datasets/20240714_node_metric_entries_more_fluct_without_operator.csv'))

    df_with_operator = normalize_data(pd.read_csv(f'datasets/{input_name}-cluster_with-operator.csv'))
    df_without_operator = normalize_data(pd.read_csv(f'datasets/{input_name}-cluster_without-operator.csv'))

    # Betrachte nur die kleinste, gemeinsame Zeit der beiden Datensätze limitiert durch max_minutes
    max_time = min(df_with_operator['relative_time_5min'].max(), df_without_operator['relative_time_5min'].max(), max_minutes)
    df_with_operator = df_with_operator[df_with_operator['relative_time_5min'] <= max_time]
    df_without_operator = df_without_operator[df_without_operator['relative_time_5min'] <= max_time]

    # Stelle sicher, dass die beiden DataFrames nach Zeit ausgerichtet sind
    merged_df = pd.merge(df_with_operator, df_without_operator, on='relative_time_5min', suffixes=('_with', '_without'))
    
    # Prozentuale Differenz der CO2-Emissionsraten für jeden Datenpunkt berechnen
    merged_df['percentage_difference'] = ((merged_df['total_moer_without'] - merged_df['total_moer_with']) / merged_df['total_moer_with']) * 100
    
    # Median und Mean der prozentualen Differenzen berechnen
    median_percentage_difference = abs(merged_df['percentage_difference'].median())
    mean_percentage_difference = abs(merged_df['percentage_difference'].mean())

    # Plotte die kumulativen CO2-Emissionen
    plt.figure(figsize=(15, 5))
    plt.plot(df_with_operator['relative_time_5min'], df_with_operator['cumulative_total_moer'], label='With CO2-Operator')
    plt.plot(df_without_operator['relative_time_5min'], df_without_operator['cumulative_total_moer'], label='Without CO2-Operator')

    # Füge die durchschnittliche Differenz als Text hinzu
    plt.text(0.5, 0.15, f'CO2-Emissions Rate Reduction (Median): {median_percentage_difference:.2f}%', horizontalalignment='center', verticalalignment='center', transform=plt.gca().transAxes)
    plt.text(0.5, 0.1, f'CO2-Emissions Rate Reduction (Mean): {mean_percentage_difference:.2f}%', horizontalalignment='center', verticalalignment='center', transform=plt.gca().transAxes)
    
    plt.xlabel('Time (Minutes)')
    plt.ylabel('Cumulative Total MOER (CO2-Emissions)')
    plt.title(f'Cumulative Total MOER (CO2-Emissions){time_label}')
    plt.legend(loc='lower right')

    plt.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.1)

    plt.savefig(f'datasets/plots/{output_name}.png')
    plt.close()

    # Plot der Verteilung der prozentualen Differenzen
    plt.figure(figsize=(10, 5))
    plt.hist(merged_df['percentage_difference'], bins=50, edgecolor='black', log=True)
    plt.xlabel('Percentage Difference')
    plt.ylabel('Frequency')
    plt.title(f'Distribution of CO2-Emissions Percentage Differences{time_label}')

    plt.subplots_adjust(left=0.08, right=0.99, top=0.95, bottom=0.1)
    
    plt.savefig(f'datasets/plots/{output_name}_hist.png')
    plt.close()

plot(input_name= 'test', output_name= 'test_7d', max_minutes= 7 * 24 * 60, time_label= ' (7 Days)')
plot(input_name= 'test', output_name= 'test_2d', max_minutes= 2 * 24 * 60, time_label= ' (2 Days)')
plot(input_name= 'test', output_name= 'test_2h', max_minutes= 2 * 60, time_label= ' (2 Hours)')

plot(input_name= 'prod', output_name= 'prod_7d', max_minutes= 7 * 24 * 60, time_label= ' (7 Days)')
plot(input_name= 'prod', output_name= 'prod_2d', max_minutes= 2 * 24 * 60, time_label= ' (2 Days)')
plot(input_name= 'prod', output_name= 'prod_2h', max_minutes= 2 * 60, time_label= ' (2 Hours)')
