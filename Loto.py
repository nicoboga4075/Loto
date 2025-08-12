import os
import zipfile
import tempfile
import pandas as pd
import requests
import bs4

USER_AGENT = "python-loto-archiver/1.0 (+https://example.com)"
FDJ_ZIP_URL = "https://www.fdj.fr/jeux-de-tirage/loto/historique"
OUT_CSV = "loto_stats.csv"
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
triplet_tirage = ["annee_numero_de_tirage", "jour_de_tirage", "date_de_tirage"]
days = {"LU":"LUNDI","MA":"MARDI","ME":"MERCREDI","JE":"JEUDI","VE":"VENDREDI","SA":"SAMEDI","DI": "DIMANCHE"}
types_loto = ["loto", "super-loto", "grand-loto"]

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

def find_archive_links():
    r = session.get(FDJ_ZIP_URL, timeout=30)
    r.raise_for_status()
    soup = bs4.BeautifulSoup(r.text, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "sto.api.fdj.fr" in href or href.lower().endswith(".zip"):
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://www.fdj.fr" + href
            links.add(href)
    print(f"\n{len(links)} liens d'archive détectés")
    return links

def process_archive_url(index, link):
    print(f"\n>>> Téléchargement n°{index} : {link}")
    r = session.get(link, timeout=60)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
        tmp_zip.write(requests.get(link, timeout=30).content)
        zip_path = tmp_zip.name
    with zipfile.ZipFile(zip_path) as z:
        file_name = z.namelist()[0]
        z.extractall(os.path.join(BASE_DIR, "datasets"))
    os.remove(zip_path)
    print(f"\nFichier téléchargé : {file_name}")
    csv_df = pd.read_csv(f"{os.getcwd()}\\datasets\\{file_name}", sep=';', decimal=',')
    print(f"\n{csv_df.head()}")
    return file_name, csv_df

def clean_year_index(row):
    s = str(row)
    match len(s):
        case 8:
            return f"{s[0:4]}-{s[5:]}"
        case 7:
            return f"{s[0:4]}-{s[4:]}"
        case 5:
            return f"20{s[0:2]}-{s[2:]}"
        case 4:
            return f"2023-{s[1:]}"

def clean_harmonize_day(row):
    if row in days:
        return days[row]
    return row

def clean_harmonize_date(row):
    if len(row) == 8:
        return f"{row[6:]}/{row[4:6]}/{row[0:4]}"
    return row

def to_iso(date_str):
    j, m, a = date_str.split("/") # ISO 8601
    return f"{a}-{m}-{j}"

def type_loto(file_name):
    file = file_name.replace(".csv","")
    if "s" in file:
        return "super-loto"
    if "g" in file or "noel" in file:
        return "grand-loto"
    return "loto"

def compute_stats(df, cols, types=None, date_min=None, date_max=None):
    if types is None:
        types = types_loto
    tirages = df[df["type_loto"].isin(types)].melt(id_vars=["date_de_tirage"], value_vars=cols, value_name="numero")[["date_de_tirage", "numero"]]
    tirages = tirages[tirages["numero"] != 0]
    if date_min:
        tirages = tirages[tirages["date_de_tirage"].apply(lambda d: to_iso(d) >= to_iso(date_min))]
    if date_max:
        tirages = tirages[tirages["date_de_tirage"].apply(lambda d: to_iso(d) <= to_iso(date_max))]
    sorties = tirages.groupby("numero").agg(
        nombre_sorties=("numero", "count"),
        derniere_sortie=("date_de_tirage", lambda x: max(pd.to_datetime(x, dayfirst=True)))
    ).reset_index()
    sorties["%_sorties"] = (sorties["nombre_sorties"] / len(tirages) * 100).round(2)
    sorties["derniere_sortie"] = sorties["derniere_sortie"].dt.strftime("%d/%m/%Y")
    return sorties

if __name__ == "__main__":
    try:
        links = find_archive_links()
    except Exception as e:
        print(f"Erreur de récupération : {e}")
    dataframes = []
    for index, link in enumerate(links, start=1):
        try:
            file_name, csv_df = process_archive_url(index, link)
            if "1er_ou_2eme_tirage" in csv_df.columns:
                df1 = csv_df[csv_df["1er_ou_2eme_tirage"] == 1].copy()
                df2 = csv_df[csv_df["1er_ou_2eme_tirage"] == 2].copy()
                df1.drop(columns=["1er_ou_2eme_tirage"], inplace=True)
                df2.drop(columns=["1er_ou_2eme_tirage"], inplace=True)
                rename_dict = {f"boule_{i}": f"boule_{i}_second_tirage" for i in range(1, 7)}
                rename_dict["boule_complementaire"] = "boule_complementaire_second_tirage"
                df2 = df2.rename(columns=rename_dict)
                csv_df = pd.merge(df1, df2, on=triplet_tirage, how='left')
            csv_df["source_file"] = file_name
            csv_df["type_loto"] = type_loto(file_name)
            columns = ["source_file", "type_loto"] + [col for col in csv_df.columns if col not in ["source_file", "type_loto"]]
            csv_df = csv_df[columns]
            dataframes.append(csv_df)
        except Exception as e:
            print(f"Erreur de lecture : {e}")
    df = pd.concat(dataframes, ignore_index=True, sort=False)
    df = df.loc[:, ~df.columns.str.contains("Unnamed|devise|forclusion|codes|joker|nombre|rapport|promotion|combinaison|_x|_y")]
    df["annee_numero_de_tirage"] = df["annee_numero_de_tirage"].astype(str).apply(clean_year_index)
    df["jour_de_tirage"] = df["jour_de_tirage"].astype(str).apply(clean_harmonize_day)
    df["date_de_tirage"] = df["date_de_tirage"].astype(str).apply(clean_harmonize_date)
    df.sort_values(by="date_de_tirage", key=lambda col: col.apply(to_iso), ascending=False, inplace=True)
    boule_cols = [col for col in df.columns if "boule_" in col or col == "numero_chance"]
    boule_premier_tirage_cols = [f"boule_{i}" for i in range(1,6)]
    boule_numero_chance_cols = ["numero_chance"]
    boule_second_tirage_cols = [f"boule_{i}_second_tirage" for i in range(1,5)]
    for col in boule_cols:
        df[col] = df[col].fillna(0).astype(int)
    df.drop_duplicates(subset=triplet_tirage, inplace=True)
    df.to_csv(f"{os.path.join(BASE_DIR,OUT_CSV)}", index=False, encoding="utf-8")
    print(f"\n>>> Fichier nettoyé -> {OUT_CSV}")
    print(f"\n{df.head()}")
    print(f"\nNombre de tirages: {len(df)}")
    print(f"\nPériode: {df['date_de_tirage'].iloc[len(df)-1]} -> {df['date_de_tirage'].iloc[0]}")
    print("\nStatistiques du premier tirage\n")
    sorties_premier_tirage = compute_stats(df, boule_premier_tirage_cols, types = ["loto","super-loto"], date_min="14/07/2019").sort_values(by="numero", ascending=True)
    print(sorties_premier_tirage)
    print("\nStatistiques du numéro chance\n")
    sorties_numero_chance = compute_stats(df, boule_numero_chance_cols, types = ["loto","super-loto"], date_min="14/07/2019").sort_values(by="numero", ascending=True)
    print(sorties_numero_chance)
    print("\nStatistiques du second tirage\n")
    sorties_second_tirage = compute_stats(df, boule_second_tirage_cols, types = ["loto","super-loto"], date_min="14/07/2019").sort_values(by="numero", ascending=True)
    print(sorties_second_tirage)
