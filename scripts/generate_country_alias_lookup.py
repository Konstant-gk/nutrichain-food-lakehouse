#!/usr/bin/env python3
"""Build country_alias_lookup.csv for Silver joins and dbt seeds.

Writes:
  - databricks/silver/data/country_alias_lookup.csv  (Silver PySpark join)
  - dbt/seeds/country_alias_lookup.csv               (dbt reference copy)

Re-run after profiling DISTINCT primary_country in Databricks; append custom rows
to databricks/silver/data/country_alias_custom.csv (optional, merged by generator).
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT_DATABRICKS = REPO / "databricks" / "silver" / "data" / "country_alias_lookup.csv"
OUT_DBT = REPO / "dbt" / "seeds" / "country_alias_lookup.csv"
CUSTOM = REPO / "databricks" / "silver" / "data" / "country_alias_custom.csv"

# ISO 3166-1 alpha-2 → English short name (official list, 249 territories).
ISO3166_EN: list[tuple[str, str]] = [
    ("AF", "Afghanistan"), ("AX", "Åland Islands"), ("AL", "Albania"), ("DZ", "Algeria"),
    ("AS", "American Samoa"), ("AD", "Andorra"), ("AO", "Angola"), ("AI", "Anguilla"),
    ("AQ", "Antarctica"), ("AG", "Antigua and Barbuda"), ("AR", "Argentina"), ("AM", "Armenia"),
    ("AW", "Aruba"), ("AU", "Australia"), ("AT", "Austria"), ("AZ", "Azerbaijan"),
    ("BS", "Bahamas"), ("BH", "Bahrain"), ("BD", "Bangladesh"), ("BB", "Barbados"),
    ("BY", "Belarus"), ("BE", "Belgium"), ("BZ", "Belize"), ("BJ", "Benin"),
    ("BM", "Bermuda"), ("BT", "Bhutan"), ("BO", "Bolivia"), ("BQ", "Bonaire"),
    ("BA", "Bosnia and Herzegovina"), ("BW", "Botswana"), ("BV", "Bouvet Island"),
    ("BR", "Brazil"), ("IO", "British Indian Ocean Territory"), ("BN", "Brunei"),
    ("BG", "Bulgaria"), ("BF", "Burkina Faso"), ("BI", "Burundi"), ("CV", "Cabo Verde"),
    ("KH", "Cambodia"), ("CM", "Cameroon"), ("CA", "Canada"), ("KY", "Cayman Islands"),
    ("CF", "Central African Republic"), ("TD", "Chad"), ("CL", "Chile"), ("CN", "China"),
    ("CX", "Christmas Island"), ("CC", "Cocos Islands"), ("CO", "Colombia"), ("KM", "Comoros"),
    ("CG", "Congo"), ("CD", "Congo (DRC)"), ("CK", "Cook Islands"), ("CR", "Costa Rica"),
    ("CI", "Côte d'Ivoire"), ("HR", "Croatia"), ("CU", "Cuba"), ("CW", "Curaçao"),
    ("CY", "Cyprus"), ("CZ", "Czechia"), ("DK", "Denmark"), ("DJ", "Djibouti"),
    ("DM", "Dominica"), ("DO", "Dominican Republic"), ("EC", "Ecuador"), ("EG", "Egypt"),
    ("SV", "El Salvador"), ("GQ", "Equatorial Guinea"), ("ER", "Eritrea"), ("EE", "Estonia"),
    ("SZ", "Eswatini"), ("ET", "Ethiopia"), ("FK", "Falkland Islands"), ("FO", "Faroe Islands"),
    ("FJ", "Fiji"), ("FI", "Finland"), ("FR", "France"), ("GF", "French Guiana"),
    ("PF", "French Polynesia"), ("TF", "French Southern Territories"), ("GA", "Gabon"),
    ("GM", "Gambia"), ("GE", "Georgia"), ("DE", "Germany"), ("GH", "Ghana"),
    ("GI", "Gibraltar"), ("GR", "Greece"), ("GL", "Greenland"), ("GD", "Grenada"),
    ("GP", "Guadeloupe"), ("GU", "Guam"), ("GT", "Guatemala"), ("GG", "Guernsey"),
    ("GN", "Guinea"), ("GW", "Guinea-Bissau"), ("GY", "Guyana"), ("HT", "Haiti"),
    ("HM", "Heard Island and McDonald Islands"), ("VA", "Holy See"), ("HN", "Honduras"),
    ("HK", "Hong Kong"), ("HU", "Hungary"), ("IS", "Iceland"), ("IN", "India"),
    ("ID", "Indonesia"), ("IR", "Iran"), ("IQ", "Iraq"), ("IE", "Ireland"),
    ("IM", "Isle of Man"), ("IL", "Israel"), ("IT", "Italy"), ("JM", "Jamaica"),
    ("JP", "Japan"), ("JE", "Jersey"), ("JO", "Jordan"), ("KZ", "Kazakhstan"),
    ("KE", "Kenya"), ("KI", "Kiribati"), ("KP", "North Korea"), ("KR", "South Korea"),
    ("KW", "Kuwait"), ("KG", "Kyrgyzstan"), ("LA", "Laos"), ("LV", "Latvia"),
    ("LB", "Lebanon"), ("LS", "Lesotho"), ("LR", "Liberia"), ("LY", "Libya"),
    ("LI", "Liechtenstein"), ("LT", "Lithuania"), ("LU", "Luxembourg"), ("MO", "Macao"),
    ("MG", "Madagascar"), ("MW", "Malawi"), ("MY", "Malaysia"), ("MV", "Maldives"),
    ("ML", "Mali"), ("MT", "Malta"), ("MH", "Marshall Islands"), ("MQ", "Martinique"),
    ("MR", "Mauritania"), ("MU", "Mauritius"), ("YT", "Mayotte"), ("MX", "Mexico"),
    ("FM", "Micronesia"), ("MD", "Moldova"), ("MC", "Monaco"), ("MN", "Mongolia"),
    ("ME", "Montenegro"), ("MS", "Montserrat"), ("MA", "Morocco"), ("MZ", "Mozambique"),
    ("MM", "Myanmar"), ("NA", "Namibia"), ("NR", "Nauru"), ("NP", "Nepal"),
    ("NL", "Netherlands"), ("NC", "New Caledonia"), ("NZ", "New Zealand"), ("NI", "Nicaragua"),
    ("NE", "Niger"), ("NG", "Nigeria"), ("NU", "Niue"), ("NF", "Norfolk Island"),
    ("MK", "North Macedonia"), ("MP", "Northern Mariana Islands"), ("NO", "Norway"),
    ("OM", "Oman"), ("PK", "Pakistan"), ("PW", "Palau"), ("PS", "Palestine"),
    ("PA", "Panama"), ("PG", "Papua New Guinea"), ("PY", "Paraguay"), ("PE", "Peru"),
    ("PH", "Philippines"), ("PN", "Pitcairn"), ("PL", "Poland"), ("PT", "Portugal"),
    ("PR", "Puerto Rico"), ("QA", "Qatar"), ("RE", "Réunion"), ("RO", "Romania"),
    ("RU", "Russia"), ("RW", "Rwanda"), ("BL", "Saint Barthélemy"), ("SH", "Saint Helena"),
    ("KN", "Saint Kitts and Nevis"), ("LC", "Saint Lucia"), ("MF", "Saint Martin"),
    ("PM", "Saint Pierre and Miquelon"), ("VC", "Saint Vincent and the Grenadines"),
    ("WS", "Samoa"), ("SM", "San Marino"), ("ST", "Sao Tome and Principe"), ("SA", "Saudi Arabia"),
    ("SN", "Senegal"), ("RS", "Serbia"), ("SC", "Seychelles"), ("SL", "Sierra Leone"),
    ("SG", "Singapore"), ("SX", "Sint Maarten"), ("SK", "Slovakia"), ("SI", "Slovenia"),
    ("SB", "Solomon Islands"), ("SO", "Somalia"), ("ZA", "South Africa"), ("GS", "South Georgia"),
    ("SS", "South Sudan"), ("ES", "Spain"), ("LK", "Sri Lanka"), ("SD", "Sudan"),
    ("SR", "Suriname"), ("SJ", "Svalbard and Jan Mayen"), ("SE", "Sweden"), ("CH", "Switzerland"),
    ("SY", "Syria"), ("TW", "Taiwan"), ("TJ", "Tajikistan"), ("TZ", "Tanzania"),
    ("TH", "Thailand"), ("TL", "Timor-Leste"), ("TG", "Togo"), ("TK", "Tokelau"),
    ("TO", "Tonga"), ("TT", "Trinidad and Tobago"), ("TN", "Tunisia"), ("TR", "Türkiye"),
    ("TM", "Turkmenistan"), ("TC", "Turks and Caicos Islands"), ("TV", "Tuvalu"),
    ("UG", "Uganda"), ("UA", "Ukraine"), ("AE", "United Arab Emirates"), ("GB", "United Kingdom"),
    ("US", "United States"), ("UM", "United States Minor Outlying Islands"), ("UY", "Uruguay"),
    ("UZ", "Uzbekistan"), ("VU", "Vanuatu"), ("VE", "Venezuela"), ("VN", "Vietnam"),
    ("VG", "British Virgin Islands"), ("VI", "U.S. Virgin Islands"), ("WF", "Wallis and Futuna"),
    ("EH", "Western Sahara"), ("YE", "Yemen"), ("ZM", "Zambia"), ("ZW", "Zimbabwe"),
]

# Common exonyms / OFF quirks → ISO (extend via country_alias_custom.csv).
EXTRA_ALIASES: list[tuple[str, str, str]] = [
    ("griechenland", "GR", "Greece"),
    ("griekenland", "GR", "Greece"),
    ("alemania", "DE", "Germany"),
    ("deutschland", "DE", "Germany"),
    ("tyskland", "DE", "Germany"),
    ("frankreich", "FR", "France"),
    ("francia", "FR", "France"),
    ("francja", "FR", "France"),
    ("frankrijk", "FR", "France"),
    ("frankrig", "FR", "France"),
    ("brésil", "BR", "Brazil"),
    ("brasil", "BR", "Brazil"),
    ("tunisie", "TN", "Tunisia"),
    ("roemenie", "RO", "Romania"),
    ("românia", "RO", "Romania"),
    ("česko", "CZ", "Czechia"),
    ("czech republic", "CZ", "Czechia"),
    ("rakousko", "AT", "Austria"),
    ("österreich", "AT", "Austria"),
    ("norwegen", "NO", "Norway"),
    ("norwegie", "NO", "Norway"),
    ("norwegan", "NO", "Norway"),
    ("norge", "NO", "Norway"),
    ("kanada", "CA", "Canada"),
    ("usa", "US", "United States"),
    ("united states of america", "US", "United States"),
    ("méxico", "MX", "Mexico"),
    ("irland", "IE", "Ireland"),
    ("uk", "GB", "United Kingdom"),
    ("great britain", "GB", "United Kingdom"),
    ("england", "GB", "United Kingdom"),
    ("scotland", "GB", "United Kingdom"),
    ("wales", "GB", "United Kingdom"),
    ("holland", "NL", "Netherlands"),
    ("nederland", "NL", "Netherlands"),
    ("belgië", "BE", "Belgium"),
    ("belgie", "BE", "Belgium"),
    ("schweiz", "CH", "Switzerland"),
    ("suisse", "CH", "Switzerland"),
    ("sverige", "SE", "Sweden"),
    ("danmark", "DK", "Denmark"),
    ("suomi", "FI", "Finland"),
    ("italia", "IT", "Italy"),
    ("italien", "IT", "Italy"),
    ("españa", "ES", "Spain"),
    ("espana", "ES", "Spain"),
    ("spanien", "ES", "Spain"),
    ("maroc", "MA", "Morocco"),
    ("marokko", "MA", "Morocco"),
    ("korea", "KR", "South Korea"),
    ("republic of korea", "KR", "South Korea"),
    ("island", "IS", "Iceland"),
    ("en:germany", "DE", "Germany"),
    ("de:germany", "DE", "Germany"),
    ("en:en:germany", "DE", "Germany"),
    ("en:south korea", "KR", "South Korea"),
    ("en:united states", "US", "United States"),
    ("en:united-kingdom", "GB", "United Kingdom"),
    ("en:france", "FR", "France"),
    ("en:italy", "IT", "Italy"),
    ("en:spain", "ES", "Spain"),
    ("en:canada", "CA", "Canada"),
    ("en:ca", "CA", "Canada"),
    ("en:us", "US", "United States"),
    ("en:au", "AU", "Australia"),
    ("en:argentina", "AR", "Argentina"),
    ("en:puerto rico", "PR", "Puerto Rico"),
    ("en:north macedonia", "MK", "North Macedonia"),
    ("en:trinidad and tobago", "TT", "Trinidad and Tobago"),
    ("en:trinidad-and-tobago", "TT", "Trinidad and Tobago"),
    ("en:virgin islands of the united states", "VI", "U.S. Virgin Islands"),
    ("en:virgin-islands-of-the-united-states", "VI", "U.S. Virgin Islands"),
    ("en:mq", "MQ", "Martinique"),
    ("en:hn", "HN", "Honduras"),
    ("en:ma", "MA", "Morocco"),
    ("en:iceland", "IS", "Iceland"),
    ("en:kazakhstan", "KZ", "Kazakhstan"),
    ("en:brunei", "BN", "Brunei"),
    ("en:sa", "SA", "Saudi Arabia"),
    ("en:zambia", "ZM", "Zambia"),
    ("en:vietnam", "VN", "Vietnam"),
    ("en:belgium", "BE", "Belgium"),
    ("en:netherlands", "NL", "Netherlands"),
    ("en:switzerland", "CH", "Switzerland"),
    ("en:sweden", "SE", "Sweden"),
    ("en:denmark", "DK", "Denmark"),
    ("en:finland", "FI", "Finland"),
    ("en:poland", "PL", "Poland"),
    ("en:portugal", "PT", "Portugal"),
    ("en:greece", "GR", "Greece"),
    ("en:russia", "RU", "Russia"),
    ("en:ukraine", "UA", "Ukraine"),
    ("en:india", "IN", "India"),
    ("en:china", "CN", "China"),
    ("en:japan", "JP", "Japan"),
    ("en:mexico", "MX", "Mexico"),
    ("en:brazil", "BR", "Brazil"),
    ("en:australia", "AU", "Australia"),
    ("en:new zealand", "NZ", "New Zealand"),
    ("en:ireland", "IE", "Ireland"),
    ("en:romania", "RO", "Romania"),
    ("en:hungary", "HU", "Hungary"),
    ("en:czech republic", "CZ", "Czechia"),
    ("en:czechia", "CZ", "Czechia"),
    ("en:austria", "AT", "Austria"),
    ("en:norway", "NO", "Norway"),
    ("en:turkey", "TR", "Türkiye"),
    ("en:türkiye", "TR", "Türkiye"),
    ("en:egypt", "EG", "Egypt"),
    ("en:israel", "IL", "Israel"),
    ("en:qatar", "QA", "Qatar"),
    ("en:south africa", "ZA", "South Africa"),
    ("en:world", "XX", "Unknown Country"),
    ("world", "XX", "Unknown Country"),
    ("unknown", "XX", "Unknown Country"),
    ("nabiał", "PL", "Poland"),
    ("nabial", "PL", "Poland"),
    ("getränke", "DE", "Germany"),
    ("wasser", "DE", "Germany"),
    ("ελλάδα", "GR", "Greece"),
    ("السودان", "SD", "Sudan"),
    ("الجزائر", "DZ", "Algeria"),
    ("مصر", "EG", "Egypt"),
    ("россия", "RU", "Russia"),
    ("україна", "UA", "Ukraine"),
    ("северна македонија", "MK", "North Macedonia"),
    ("ivory coast", "CI", "Côte d'Ivoire"),
    ("cote d'ivoire", "CI", "Côte d'Ivoire"),
    ("burma", "MM", "Myanmar"),
    ("east timor", "TL", "Timor-Leste"),
    ("cape verde", "CV", "Cabo Verde"),
    ("swaziland", "SZ", "Eswatini"),
    ("macedonia", "MK", "North Macedonia"),
    ("czechia", "CZ", "Czechia"),
    ("türkiye", "TR", "Türkiye"),
    ("turkey", "TR", "Türkiye"),
    ("laos", "LA", "Laos"),
    ("viet nam", "VN", "Vietnam"),
    ("u.s.", "US", "United States"),
    ("u.s.a.", "US", "United States"),
    ("u.k.", "GB", "United Kingdom"),
]


def _slug(name: str) -> str:
    return re.sub(r"\s+", "-", name.strip().lower())


def _aliases_for_iso(iso: str, display: str) -> set[tuple[str, str, str]]:
    iso = iso.upper()
    display = display.strip()
    keys: set[str] = set()
    keys.add(iso.lower())
    keys.add(display.lower())
    keys.add(_slug(display))
    keys.add(f"en:{display.lower()}")
    keys.add(f"en:{_slug(display)}")
    if iso == "GB":
        keys.update({"uk", "great britain", "england", "scotland", "wales", "en:united-kingdom"})
    if iso == "US":
        keys.update({"usa", "u.s.", "u.s.a.", "en:united-states"})
    if iso == "KR":
        keys.update({"korea", "republic of korea", "en:south-korea"})
    if iso == "CZ":
        keys.update({"czech republic", "czechia", "česko"})
    if iso == "TR":
        keys.update({"turkey", "türkiye"})
    rows: set[tuple[str, str, str]] = set()
    for k in keys:
        k = k.strip()
        if k:
            rows.add((k, iso, display))
    return rows


def build_rows() -> list[tuple[str, str, str]]:
    rows: dict[str, tuple[str, str, str]] = {}
    for iso, display in ISO3166_EN:
        for alias, code, name in _aliases_for_iso(iso, display):
            rows[alias] = (alias, code, name)
    for alias, code, name in EXTRA_ALIASES:
        rows[alias.lower().strip()] = (alias.lower().strip(), code.upper(), name)
    if CUSTOM.is_file():
        with CUSTOM.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                alias_val = (row.get("alias") or "").strip()
                if not alias_val or alias_val.startswith("#"):
                    continue
                a = alias_val.lower()
                rows[a] = (a, row["iso_code"].upper().strip(), row["display_name"].strip())
    return sorted(rows.values(), key=lambda r: (r[1], r[0]))


def write_csv(path: Path, rows: list[tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["alias", "iso_code", "display_name"])
        w.writerows(rows)


def main() -> None:
    rows = build_rows()
    write_csv(OUT_DATABRICKS, rows)
    write_csv(OUT_DBT, rows)
    print(f"Wrote {len(rows)} aliases to {OUT_DATABRICKS}")
    print(f"Wrote {len(rows)} aliases to {OUT_DBT}")


if __name__ == "__main__":
    main()
