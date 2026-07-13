"""Materials registry for the Commodity Markets tab.

One entry per trackable raw material from the user's materials index
(Table A of materials_unique_index.md). Rare earths are tracked as one
basket entry (individual REEs are co-products of the same ores and have
no separate country production data); krypton & xenon are combined as in
the index.

Per material:
  name / symbol / category / sourcing (flag + note from the index) / uses
  usgs   — production stages, each with match rules against the two USGS
           Mineral Commodity Summaries data-release formats:
             mcs2026: (Commodity, Statistics_detail prefix)   [years 2024, 2025e]
             mcs2025: (COMMODITY,  TYPE prefix)               [adds 2023]
           Stage keys: "mine" | "refinery" | "smelter" | "production" | custom.
  hs     — [(HS code, label), ...] traded forms, most representative first.
           Codes may be shared between materials (e.g. 8112.92 = unwrought
           Ga/Ge/In/Nb/V; 2804.29 = rare gases): fetched once, referenced.
  fred   — FRED series id for a monthly benchmark price (IMF PCPS), or None.
  yahoo  — (ticker, unit, label) for a daily exchange price, or None.

Materials with no USGS world-production table carry a "prod_note" explaining
why (e.g. pure byproducts with no tracked mine output of their own).
"""

CATEGORIES = [
    "Base & structural metals",
    "Precious & platinum-group metals",
    "Rare-earth elements",
    "Battery actives, non-metals & light elements",
    "Compound-semiconductor source metals",
    "Industrial gases",
]

# Trade series start years (Comtrade calls are slow — new materials start 2022;
# copper keeps its original 2020 start so existing data is preserved).
TRADE_START_DEFAULT = 2022
TRADE_START = {"7403": 2020, "2603": 2020}

MATERIALS = {
    # ------------------------------------------------ base & structural metals
    "copper": {
        "name": "Copper", "symbol": "Cu", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "Host for Se, Te, Mo, Re, Au byproducts.",
        "uses": "Conductors, windings, busbars, current collectors, chip interconnect, contacts.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Copper", "Mine production"),
                     "mcs2025": ("Copper", "Mine production")},
            "refinery": {"label": "Refinery production", "mcs2026": ("Copper", "Refinery production"),
                         "mcs2025": ("Copper", "Refinery production")},
        },
        "hs": [("7403", "Refined copper & alloys, unwrought"),
               ("2603", "Copper ores & concentrates")],
        "fred": ("PCOPPUSDM", "Global copper benchmark price (IMF, LME-basis)", "USD per metric ton"),
        "yahoo": ("HG=F", "USD per pound", "COMEX copper futures, front month"),
    },
    "aluminium": {
        "name": "Aluminium", "symbol": "Al", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "Bauxite → alumina → smelting; host for Ga byproduct.",
        "uses": "ACSR conductors, foils, frames, heat sinks, casings, structure.",
        "usgs": {
            "mine": {"label": "Bauxite mine production", "mcs2026": ("Bauxite", "Bauxite"),
                     "mcs2025": ("Bauxite", "Mine production")},
            "refinery": {"label": "Alumina refinery production", "mcs2026": ("Alumina", "Alumina"),
                         "mcs2025": ("Bauxite", "Refinery production, alumina")},
            "smelter": {"label": "Primary aluminium smelter production",
                        "mcs2026": ("Aluminum", "Smelter production"),
                        "mcs2025": ("Aluminum", "Smelter production")},
        },
        "hs": [("7601", "Unwrought aluminium"),
               ("2606", "Aluminium ores & concentrates (bauxite)")],
        "fred": ("PALUMUSDM", "Global aluminium benchmark price (IMF, LME-basis)", "USD per metric ton"),
        "yahoo": ("ALI=F", "USD per metric ton", "COMEX aluminium futures, front month"),
    },
    "iron": {
        "name": "Iron", "symbol": "Fe", "category": "Base & structural metals",
        "sourcing": "Primary",
        "uses": "Steel everywhere (towers, cores, rebar, vessels); iron-flow battery electrolyte.",
        "usgs": {
            "mine": {"label": "Iron ore mine production (iron content)",
                     "mcs2026": ("Iron Ore", "Mine production: Iron content"),
                     "mcs2025": ("Iron Ore", "Mine production, iron content")},
        },
        "hs": [("2601", "Iron ores & concentrates"),
               ("7201", "Pig iron")],
        "fred": ("PIORECRUSDM", "Global iron ore benchmark price (IMF, 62% Fe CFR China)", "USD per metric ton"),
        "yahoo": None,
    },
    "nickel": {
        "name": "Nickel", "symbol": "Ni", "category": "Base & structural metals",
        "sourcing": "Primary",
        "uses": "NMC/NCA cathodes, superalloys, SOFC, plating, alkaline electrolysers.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Nickel", "Mine production"),
                     "mcs2025": ("Nickel", "Mine production")},
        },
        "hs": [("7502", "Unwrought nickel"),
               ("2604", "Nickel ores & concentrates")],
        "fred": ("PNICKUSDM", "Global nickel benchmark price (IMF, LME-basis)", "USD per metric ton"),
        "yahoo": None,
    },
    "cobalt": {
        "name": "Cobalt", "symbol": "Co", "category": "Base & structural metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from Cu (DRC) and Ni ores.",
        "uses": "NMC/NCA cathodes, superalloy turbine blades, HDD media.",
        "usgs": {
            "mine": {"label": "Mine production (cobalt content)", "mcs2026": ("Cobalt", "Mine production"),
                     "mcs2025": ("Cobalt", "Mine production")},
        },
        "hs": [("8105", "Cobalt mattes, intermediates, metal & articles"),
               ("2605", "Cobalt ores & concentrates")],
        "fred": None, "yahoo": None,
    },
    "manganese": {
        "name": "Manganese", "symbol": "Mn", "category": "Base & structural metals",
        "sourcing": "Primary",
        "uses": "NMC/LMO/LMFP cathodes, steel alloying.",
        "usgs": {
            "mine": {"label": "Mine production (Mn content)", "mcs2026": ("Manganese", "Mine production"),
                     "mcs2025": ("Manganese", "Mine production")},
        },
        "hs": [("2602", "Manganese ores & concentrates"),
               ("8111", "Manganese & articles")],
        "fred": None, "yahoo": None,
    },
    "zinc": {
        "name": "Zinc", "symbol": "Zn", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "Host for In, Ge, Cd byproducts.",
        "uses": "Galvanising; Zn-bromine / Zn-ion / Zn-air batteries.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Zinc", "Mine production"),
                     "mcs2025": ("Zinc", "Mine production")},
        },
        "hs": [("7901", "Unwrought zinc"),
               ("2608", "Zinc ores & concentrates")],
        "fred": ("PZINCUSDM", "Global zinc benchmark price (IMF, LME-basis)", "USD per metric ton"),
        "yahoo": None,
    },
    "lead": {
        "name": "Lead", "symbol": "Pb", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "Host for Bi byproduct; often mined with Zn.",
        "uses": "Lead-acid batteries, cable sheathing.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Lead", "Mine production"),
                     "mcs2025": ("Lead", "Mine production")},
        },
        "hs": [("7801", "Unwrought lead"),
               ("2607", "Lead ores & concentrates")],
        "fred": ("PLEADUSDM", "Global lead benchmark price (IMF, LME-basis)", "USD per metric ton"),
        "yahoo": None,
    },
    "tin": {
        "name": "Tin", "symbol": "Sn", "category": "Base & structural metals",
        "sourcing": "Primary",
        "uses": "Solder (SAC, Sn-Bi), EUV light-source droplets.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Tin", "Mine production"),
                     "mcs2025": ("Tin", "Mine production")},
        },
        "hs": [("8001", "Unwrought tin"),
               ("2609", "Tin ores & concentrates")],
        "fred": ("PTINUSDM", "Global tin benchmark price (IMF, LME-basis)", "USD per metric ton"),
        "yahoo": None,
    },
    "titanium": {
        "name": "Titanium", "symbol": "Ti", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "From ilmenite / rutile mineral sands.",
        "uses": "Corrosion alloys, PEM fuel-cell plates, LTO anode; TiO₂ pigment feedstock.",
        "usgs": {
            "mine": {"label": "Ilmenite mine production",
                     "mcs2026": ("Titanium Mineral Concentrates", "Mine production: Ilmenite"),
                     "mcs2025": ("Titanium Mineral Concentrates", "Mine production, ilmen")},
            "mine2": {"label": "Rutile mine production",
                      "mcs2026": ("Titanium Mineral Concentrates", "Mine production: Rutile"),
                      "mcs2025": ("Titanium Mineral Concentrates", "Mine production, rutile")},
            "refinery": {"label": "Titanium sponge metal production",
                         "mcs2026": ("Titanium Sponge Metal", "Titanium sponge metal production"),
                         "mcs2025": ("Titanium & titanium dioxide", "Sponge")},
        },
        "hs": [("2614", "Titanium ores & concentrates"),
               ("8108", "Titanium & articles")],
        "fred": None, "yahoo": None,
    },
    "chromium": {
        "name": "Chromium", "symbol": "Cr", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "From chromite (FeCr₂O₄).",
        "uses": "Stainless / Cr-Mo steels, vacuum-interrupter contacts, photomask absorber.",
        "usgs": {
            "mine": {"label": "Chromite mine production", "mcs2026": ("Chromium", "Mine production"),
                     "mcs2025": ("Chromium", "Mine production")},
        },
        "hs": [("2610", "Chromium ores & concentrates"),
               ("720241", "Ferro-chromium (>4% carbon)")],
        "fred": None, "yahoo": None,
    },
    "molybdenum": {
        "name": "Molybdenum", "symbol": "Mo", "category": "Base & structural metals",
        "sourcing": "Primary + byproduct", "sourcing_note": "Own mines plus byproduct of Cu porphyry.",
        "uses": "Arc contacts, EUV Mo/Si mirrors & masks, superalloys.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Molybdenum", "Mine production"),
                     "mcs2025": ("Molybdenum", "Mine production")},
        },
        "hs": [("2613", "Molybdenum ores & concentrates"),
               ("8102", "Molybdenum & articles")],
        "fred": None, "yahoo": None,
    },
    "tungsten": {
        "name": "Tungsten", "symbol": "W", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "From wolframite / scheelite.",
        "uses": "Chip contacts/vias, arc contacts, superalloys, tooling.",
        "usgs": {
            "mine": {"label": "Mine production (W content)", "mcs2026": ("Tungsten", "Mine production"),
                     "mcs2025": ("Tungsten", "Mine production")},
        },
        "hs": [("2611", "Tungsten ores & concentrates"),
               ("8101", "Tungsten & articles")],
        "fred": None, "yahoo": None,
    },
    "vanadium": {
        "name": "Vanadium", "symbol": "V", "category": "Base & structural metals",
        "sourcing": "Co-product", "sourcing_note": "From titanomagnetite / steel slag; some primary.",
        "uses": "VRFB flow-battery electrolyte, steel microalloying.",
        "usgs": {
            "mine": {"label": "Mine production (V content)", "mcs2026": ("Vanadium", "Mine production"),
                     "mcs2025": ("Vanadium", "Mine production")},
        },
        "hs": [("282530", "Vanadium oxides & hydroxides")],
        "fred": None, "yahoo": None,
    },
    "tantalum": {
        "name": "Tantalum", "symbol": "Ta", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "From tantalite / coltan; some Sn-slag co-product.",
        "uses": "Chip barrier metal (Ta/TaN), MLCC & Ta capacitors.",
        "usgs": {
            "mine": {"label": "Mine production (Ta content)", "mcs2026": ("Tantalum", "Mine production"),
                     "mcs2025": ("Tantalum", "Mine production")},
        },
        "hs": [("8103", "Tantalum & articles"),
               ("261590", "Niobium, tantalum & vanadium ores (shared code)")],
        "fred": None, "yahoo": None,
    },
    "niobium": {
        "name": "Niobium", "symbol": "Nb", "category": "Base & structural metals",
        "sourcing": "Primary", "sourcing_note": "From pyrochlore (Brazil dominates).",
        "uses": "Capacitors, steel microalloying, NbTi superconductors.",
        "usgs": {
            "mine": {"label": "Mine production (Nb content)",
                     "mcs2026": ("Niobium (Columbium)", "Mine production"),
                     "mcs2025": ("Niobium", "Mine production")},
        },
        "hs": [("720293", "Ferro-niobium"),
               ("261590", "Niobium, tantalum & vanadium ores (shared code)")],
        "fred": None, "yahoo": None,
    },
    "hafnium": {
        "name": "Hafnium", "symbol": "Hf", "category": "Base & structural metals",
        "sourcing": "Byproduct", "sourcing_note": "Separated from zirconium during Zr refining.",
        "uses": "High-k gate dielectric (HfO₂), nuclear control rods, superalloys.",
        "usgs": {},
        "prod_note": "No per-country production data: hafnium is recovered only during zirconium "
                     "refining — see zirconium for the host mineral supply.",
        "hs": [("811231", "Hafnium, unwrought; powders")],
        "fred": None, "yahoo": None,
    },
    "zirconium": {
        "name": "Zirconium", "symbol": "Zr", "category": "Base & structural metals",
        "sourcing": "Co-product", "sourcing_note": "Zircon from Ti mineral sands; host for Hf.",
        "uses": "Zircaloy fuel cladding, high-k precursor, LLZO solid electrolyte.",
        "usgs": {
            "mine": {"label": "Zirconium mineral concentrates mine production",
                     "mcs2026": ("Zirconium", "Zirconium mineral concentrates, mine production"),
                     "mcs2025": ("Zirconium and Hafnium", "Mine production")},
        },
        "hs": [("261510", "Zirconium ores & concentrates"),
               ("8109", "Zirconium & articles")],
        "fred": None, "yahoo": None,
    },
    "antimony": {
        "name": "Antimony", "symbol": "Sb", "category": "Base & structural metals",
        "sourcing": "Primary + byproduct", "sourcing_note": "Stibnite; also from Pb/Au processing.",
        "uses": "Lead-acid alloy, liquid-metal (Ca/Sb) battery, flame retardant, dopant.",
        "usgs": {
            "mine": {"label": "Mine production (Sb content)", "mcs2026": ("Antimony", "Mine production"),
                     "mcs2025": ("Antimony", "Mine production")},
        },
        "hs": [("261710", "Antimony ores & concentrates"),
               ("8110", "Antimony & articles")],
        "fred": None, "yahoo": None,
    },
    "bismuth": {
        "name": "Bismuth", "symbol": "Bi", "category": "Base & structural metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from Pb (also Cu/W/Sn) refining.",
        "uses": "BSCCO superconductor, low-temperature Sn-Bi solder.",
        "usgs": {
            "refinery": {"label": "Refinery production", "mcs2026": ("Bismuth", "Refinery production"),
                         "mcs2025": ("Bismuth", "Refinery production")},
        },
        "hs": [("8106", "Bismuth & articles")],
        "fred": None, "yahoo": None,
    },
    # ------------------------------------------- precious & platinum-group
    "silver": {
        "name": "Silver", "symbol": "Ag", "category": "Precious & platinum-group metals",
        "sourcing": "Byproduct / co-product", "sourcing_note": "Recovered with Pb-Zn, Cu and Au ores.",
        "uses": "Solar PV contacts, switch contacts, die-attach sinter, plating.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Silver", "Mine production"),
                     "mcs2025": ("Silver", "Mine production")},
        },
        "hs": [("7106", "Silver, unwrought or semi-manufactured")],
        "fred": None,
        "yahoo": ("SI=F", "USD per troy ounce", "COMEX silver futures, front month"),
    },
    "gold": {
        "name": "Gold", "symbol": "Au", "category": "Precious & platinum-group metals",
        "sourcing": "Primary", "sourcing_note": "Some byproduct of Cu mining.",
        "uses": "Wire bonding, plating, connectors.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Gold", "Mine production"),
                     "mcs2025": ("Gold", "Mine production")},
        },
        "hs": [("7108", "Gold, unwrought or semi-manufactured")],
        "fred": None,
        "yahoo": ("GC=F", "USD per troy ounce", "COMEX gold futures, front month"),
    },
    "platinum": {
        "name": "Platinum", "symbol": "Pt", "category": "Precious & platinum-group metals",
        "sourcing": "Primary + byproduct", "sourcing_note": "PGM reef ore (Bushveld); Ni-Cu byproduct.",
        "uses": "PEM fuel-cell / electrolyser cathode catalyst.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Platinum", "Mine production: Platinum"),
                     "mcs2025": ("Platinum-Group metals", "Mine production, platinum")},
        },
        "hs": [("711011", "Platinum, unwrought or powder")],
        "fred": None,
        "yahoo": ("PL=F", "USD per troy ounce", "NYMEX platinum futures, front month"),
    },
    "palladium": {
        "name": "Palladium", "symbol": "Pd", "category": "Precious & platinum-group metals",
        "sourcing": "Primary + byproduct", "sourcing_note": "PGM reef ore; Ni-Cu byproduct.",
        "uses": "MLCC electrodes (legacy), chip-package plating.",
        "usgs": {
            "mine": {"label": "Mine production", "mcs2026": ("Palladium", "Mine production: Palladium"),
                     "mcs2025": ("Platinum-Group metals", "Mine production, palladium")},
        },
        "hs": [("711021", "Palladium, unwrought or powder")],
        "fred": None,
        "yahoo": ("PA=F", "USD per troy ounce", "NYMEX palladium futures, front month"),
    },
    "iridium": {
        "name": "Iridium", "symbol": "Ir", "category": "Precious & platinum-group metals",
        "sourcing": "Byproduct / co-product", "sourcing_note": "Small fraction of PGM mining.",
        "uses": "PEM electrolyser anode (OER) catalyst.",
        "usgs": {},
        "prod_note": "No per-country production data: iridium is a minor co-product of PGM mining "
                     "(South Africa dominant) — see platinum for the host supply.",
        "hs": [("711041", "Iridium, osmium & ruthenium, unwrought or powder (shared code)")],
        "fred": None, "yahoo": None,
    },
    "ruthenium": {
        "name": "Ruthenium", "symbol": "Ru", "category": "Precious & platinum-group metals",
        "sourcing": "Byproduct / co-product", "sourcing_note": "Fraction of PGM mining.",
        "uses": "HDD media underlayer, EUV mirror coatings, advanced-node interconnect.",
        "usgs": {},
        "prod_note": "No per-country production data: ruthenium is a minor co-product of PGM mining "
                     "(South Africa dominant) — see platinum for the host supply.",
        "hs": [("711041", "Iridium, osmium & ruthenium, unwrought or powder (shared code)")],
        "fred": None, "yahoo": None,
    },
    "rhenium": {
        "name": "Rhenium", "symbol": "Re", "category": "Precious & platinum-group metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered during molybdenite roasting (Mo often of Cu).",
        "uses": "Single-crystal superalloy turbine blades.",
        "usgs": {
            "mine": {"label": "Mine production (Re content)", "mcs2026": ("Rhenium", "Mine production"),
                     "mcs2025": ("Rhenium", "Mine production")},
        },
        "hs": [("811241", "Rhenium, unwrought; powders")],
        "fred": None, "yahoo": None,
    },
    # ------------------------------------------------------- rare earths
    "rare-earths": {
        "name": "Rare earths (basket)", "symbol": "REE", "category": "Rare-earth elements",
        "sourcing": "Co-product (REE basket)",
        "sourcing_note": "Nd, Pr, Dy, Tb, Sm, Y, La, Ce, Er, Yb are co-products of the same ores "
                         "(bastnäsite, monazite, ionic-adsorption clays); heavy REEs (Dy, Tb) are "
                         "scarce co-products. No element-level country production data exists — "
                         "the basket is tracked here.",
        "uses": "NdFeB magnets (Nd, Pr, Dy, Tb), SmCo magnets, YSZ & REBCO (Y), LLZO/SOFC (La), "
                "CMP slurry (Ce), fibre amplifiers (Er, Yb).",
        "usgs": {
            "mine": {"label": "Mine production (REO equivalent)", "mcs2026": ("Rare Earths", "Mine production"),
                     "mcs2025": ("Rare earths", "Mine production")},
        },
        "hs": [("2846", "Rare-earth compounds"),
               ("280530", "Rare-earth metals, scandium & yttrium")],
        "fred": None, "yahoo": None,
    },
    # --------------------------- battery actives, non-metals & light elements
    "lithium": {
        "name": "Lithium", "symbol": "Li", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary", "sourcing_note": "Spodumene (hard rock) and brine (salars).",
        "uses": "All Li-ion chemistries (cathode, salt); solid-state Li-metal.",
        "usgs": {
            "mine": {"label": "Mine production (Li content)", "mcs2026": ("Lithium", "Mine production"),
                     "mcs2025": ("Lithium", "Mine production")},
        },
        "hs": [("283691", "Lithium carbonate"),
               ("282520", "Lithium oxide & hydroxide")],
        "fred": None, "yahoo": None,
    },
    "sodium": {
        "name": "Sodium", "symbol": "Na", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary (abundant)", "sourcing_note": "From salt (NaCl) / brine / soda ash. Tracked via salt.",
        "uses": "Na-ion & NaS batteries, molten nitrate (CSP), chemicals.",
        "usgs": {
            "mine": {"label": "Salt production", "mcs2026": ("Salt", "Mine production"),
                     "mcs2025": ("Salt", "Mine production")},
        },
        "hs": [("2501", "Salt (sodium chloride)")],
        "fred": None, "yahoo": None,
    },
    "potassium": {
        "name": "Potassium", "symbol": "K", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary", "sourcing_note": "From potash (sylvite / brine). Tracked via potash.",
        "uses": "CSP molten nitrate (KNO₃), chemicals, fertilisers.",
        "usgs": {
            "mine": {"label": "Potash mine production (K₂O equivalent)",
                     "mcs2026": ("Potash", "Mine production"),
                     "mcs2025": ("Potash", "Mine production")},
        },
        "hs": [("3104", "Potassic fertilisers (potash)")],
        "fred": None, "yahoo": None,
    },
    "calcium": {
        "name": "Calcium", "symbol": "Ca", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary (abundant)", "sourcing_note": "From limestone (CaCO₃). Tracked via lime.",
        "uses": "Liquid-metal battery (Ca/Sb), Pb-Ca alloy, cement/lime.",
        "usgs": {
            "production": {"label": "Lime production", "mcs2026": ("Lime", "Production"),
                           "mcs2025": ("Lime", "Plant production")},
        },
        "hs": [("2522", "Quicklime, slaked lime & hydraulic lime")],
        "fred": None, "yahoo": None,
    },
    "graphite": {
        "name": "Graphite", "symbol": "C", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary (natural) / synthesised",
        "sourcing_note": "Natural flake ore, or synthetic from petroleum needle coke.",
        "uses": "Li-ion anode (natural + synthetic), electrodes, refractory.",
        "usgs": {
            "mine": {"label": "Natural graphite mine production",
                     "mcs2026": ("Graphite (Natural)", "Mine production"),
                     "mcs2025": ("Graphite", "Mine production")},
        },
        "hs": [("2504", "Natural graphite"),
               ("3801", "Artificial graphite & preparations")],
        "fred": None, "yahoo": None,
    },
    "silicon": {
        "name": "Silicon", "symbol": "Si", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary", "sourcing_note": "Quartz → metallurgical Si → polysilicon; chokepoint is purity/processing.",
        "uses": "Solar PV (polysilicon→wafer), semiconductor devices, Si anode, Fe-Si steel.",
        "usgs": {
            "smelter": {"label": "Silicon metal production", "mcs2026": ("Silicon", "Silicon metal"),
                        "mcs2025": ("Silicon", "Plant production, silicon")},
            "smelter2": {"label": "Ferrosilicon production (Si content)",
                         "mcs2026": ("Silicon", "Ferrosilicon"),
                         "mcs2025": ("Silicon", "Plant production, fero")},
        },
        "hs": [("280461", "Silicon ≥99.99% pure (polysilicon)"),
               ("280469", "Silicon <99.99% (metallurgical grade)")],
        "fred": None, "yahoo": None,
    },
    "hp-quartz": {
        "name": "High-purity quartz", "symbol": "SiO₂", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary (geographically concentrated)",
        "sourcing_note": "Mined high-purity deposits (Spruce Pine, NC dominates crucible-grade supply).",
        "uses": "Crucibles for Si crystal growth, optical-fibre feedstock, fused-silica masks/optics.",
        "usgs": {},
        "prod_note": "No per-country production data: USGS tracks only US high-purity quartz "
                     "statistics; the crucible-grade supply is dominated by Spruce Pine (US).",
        "hs": [("2506", "Quartz & quartzite")],
        "fred": None, "yahoo": None,
    },
    "phosphorus": {
        "name": "Phosphorus / phosphate", "symbol": "P", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary", "sourcing_note": "From phosphate rock.",
        "uses": "LFP/LMFP cathode, LiPF₆ salt, InP, dopant.",
        "usgs": {
            "mine": {"label": "Phosphate rock mine production",
                     "mcs2026": ("Phosphate Rock", "Mine production"),
                     "mcs2025": ("Phosphate rock", "Mine production")},
        },
        "hs": [("2510", "Natural calcium phosphates")],
        "fred": None, "yahoo": None,
    },
    "sulfur": {
        "name": "Sulfur", "symbol": "S", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from oil & gas desulfurisation and smelting.",
        "uses": "Li-S (frontier), NaS battery, H₂SO₄ (lead-acid, fab clean, VRFB).",
        "usgs": {
            "production": {"label": "Production, all forms", "mcs2026": ("Sulfur", "Production"),
                           "mcs2025": ("Sulfur", "Production")},
        },
        "hs": [("2503", "Sulfur of all kinds")],
        "fred": None, "yahoo": None,
    },
    "fluorine": {
        "name": "Fluorine / fluorspar", "symbol": "F", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary + byproduct", "sourcing_note": "Fluorspar (CaF₂); also fluorosilicic acid from phosphate.",
        "uses": "LiPF₆, PVDF, PFSA membranes, SF₆, etch gases.",
        "usgs": {
            "mine": {"label": "Fluorspar mine production", "mcs2026": ("Fluorspar", "Mine production"),
                     "mcs2025": ("Fluorspar", "Mine production")},
        },
        "hs": [("252922", "Fluorspar >97% CaF₂ (acid grade)"),
               ("252921", "Fluorspar ≤97% CaF₂ (metallurgical grade)")],
        "fred": None, "yahoo": None,
    },
    "boron": {
        "name": "Boron", "symbol": "B", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary", "sourcing_note": "Borate minerals (borax, colemanite); Turkiye + US dominate.",
        "uses": "NdFeB magnets, neutron absorbers, fibreglass, amorphous metal, BF₃ dopant.",
        "usgs": {
            "mine": {"label": "Production (all forms)", "mcs2026": ("Boron", "Production"),
                     "mcs2025": ("Boron", "Boron all types")},
        },
        "hs": [("2528", "Natural borates & concentrates")],
        "fred": None, "yahoo": None,
    },
    "bromine": {
        "name": "Bromine", "symbol": "Br", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary (from brine)", "sourcing_note": "Bromide-rich brines (Dead Sea, Arkansas).",
        "uses": "Zinc-bromine flow battery, flame retardants.",
        "usgs": {
            "production": {"label": "Production", "mcs2026": ("Bromine", "Production"),
                           "mcs2025": ("Bromine", "Production")},
        },
        "hs": [("280130", "Fluorine; bromine (shared code)")],
        "fred": None, "yahoo": None,
    },
    "chlorine": {
        "name": "Chlorine", "symbol": "Cl", "category": "Battery actives, non-metals & light elements",
        "sourcing": "Primary (from salt)", "sourcing_note": "Via chlor-alkali electrolysis of NaCl.",
        "uses": "Etch gases (Cl₂, BCl₃), ZEBRA NiCl₂ battery, chemicals.",
        "usgs": {},
        "prod_note": "No per-country production data: chlorine is made by chlor-alkali electrolysis "
                     "wherever salt and power are available — see sodium (salt) for feedstock supply.",
        "hs": [("280110", "Chlorine")],
        "fred": None, "yahoo": None,
    },
    # ------------------------------- compound-semiconductor source metals
    "gallium": {
        "name": "Gallium", "symbol": "Ga", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from bauxite/alumina (Bayer liquor); minor Zn.",
        "uses": "GaN (RF/power/LED), GaAs (RF/VCSEL), CIGS solar.",
        "usgs": {
            "production": {"label": "Primary (low-purity) production",
                           "mcs2026": ("Gallium", "Primary production"),
                           "mcs2025": ("Gallium", "Primary production")},
        },
        "hs": [("811292", "Unwrought Ga/Ge/In/Nb/V (shared code)")],
        "fred": None, "yahoo": None,
    },
    "germanium": {
        "name": "Germanium", "symbol": "Ge", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from Zn smelting; coal fly ash.",
        "uses": "Fibre-optic core doping, IR optics, SiGe.",
        "usgs": {},
        "prod_note": "No per-country production data: USGS withholds germanium output by country "
                     "(company-proprietary); China dominates refined supply — see zinc for the host supply.",
        "hs": [("811292", "Unwrought Ga/Ge/In/Nb/V (shared code)")],
        "fred": None, "yahoo": None,
    },
    "indium": {
        "name": "Indium", "symbol": "In", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from zinc smelting residues.",
        "uses": "ITO transparent conductors, CIGS solar, low-temp solder/TIM.",
        "usgs": {
            "refinery": {"label": "Refinery production", "mcs2026": ("Indium", "Refinery production"),
                         "mcs2025": ("Indium", "Refinery production")},
        },
        "hs": [("811292", "Unwrought Ga/Ge/In/Nb/V (shared code)")],
        "fred": None, "yahoo": None,
    },
    "arsenic": {
        "name": "Arsenic", "symbol": "As", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from Cu & Pb smelting.",
        "uses": "GaAs / InGaAs semiconductors, dopant.",
        "usgs": {
            "production": {"label": "Production (As₂O₃ gross weight)",
                           "mcs2026": ("Arsenic", "Production"),
                           "mcs2025": ("Arsenic", "Plant production")},
        },
        "hs": [("280480", "Arsenic")],
        "fred": None, "yahoo": None,
    },
    "selenium": {
        "name": "Selenium", "symbol": "Se", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "From copper-refining anode slimes.",
        "uses": "CIGS solar, glass, metallurgy.",
        "usgs": {
            "refinery": {"label": "Refinery production", "mcs2026": ("Selenium", "Refinery production"),
                         "mcs2025": ("Selenium", "Refinery production")},
        },
        "hs": [("280490", "Selenium")],
        "fred": None, "yahoo": None,
    },
    "tellurium": {
        "name": "Tellurium", "symbol": "Te", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "From copper-refining anode slimes.",
        "uses": "CdTe thin-film solar, thermoelectrics.",
        "usgs": {
            "refinery": {"label": "Refinery production", "mcs2026": ("Tellurium", "Refinery production"),
                         "mcs2025": ("Tellurium", "Refinery production")},
        },
        "hs": [("280450", "Boron; tellurium (shared code)")],
        "fred": None, "yahoo": None,
    },
    "cadmium": {
        "name": "Cadmium", "symbol": "Cd", "category": "Compound-semiconductor source metals",
        "sourcing": "Byproduct", "sourcing_note": "Recovered from zinc smelting.",
        "uses": "CdTe solar, pigments, Ni-Cd (legacy).",
        "usgs": {
            "refinery": {"label": "Refinery production", "mcs2026": ("Cadmium", "Refinery production"),
                         "mcs2025": ("Cadmium", "Refinery production")},
        },
        "hs": [("811261", "Cadmium, unwrought; powders")],
        "fred": None, "yahoo": None,
    },
    # ------------------------------------------------------ industrial gases
    "helium": {
        "name": "Helium", "symbol": "He", "category": "Industrial gases",
        "sourcing": "Byproduct", "sourcing_note": "Extracted from natural gas.",
        "uses": "HDD fill, semiconductor process & cooling, cryogenics (HTS, MRI).",
        "usgs": {
            "production": {"label": "Production", "mcs2026": ("Helium", "Helium Production"),
                           "mcs2025": ("Helium", "Production, helium")},
        },
        "hs": [("280429", "Rare gases other than argon (shared code)")],
        "fred": None, "yahoo": None,
    },
    "neon": {
        "name": "Neon", "symbol": "Ne", "category": "Industrial gases",
        "sourcing": "Byproduct (air separation)", "sourcing_note": "Cryogenic air separation; historically Ukraine/Russia purification.",
        "uses": "Excimer (DUV) lithography laser gas.",
        "usgs": {
            "production": {"label": "Rare gases production", "mcs2026": ("Neon", "Rare gases production"),
                           "mcs2025": None},
        },
        "prod_note": "USGS reports only US rare-gas output; global neon capacity is not tracked by country.",
        "hs": [("280429", "Rare gases other than argon (shared code)")],
        "fred": None, "yahoo": None,
    },
    "krypton-xenon": {
        "name": "Krypton & xenon", "symbol": "Kr, Xe", "category": "Industrial gases",
        "sourcing": "Byproduct (air separation)", "sourcing_note": "Cryogenic air separation.",
        "uses": "Lithography / etch, lighting, window insulation.",
        "usgs": {
            "production": {"label": "Rare gases production", "mcs2026": ("Krypton", "Rare gases production"),
                           "mcs2025": None},
        },
        "prod_note": "USGS reports only US rare-gas output; global Kr/Xe capacity is not tracked by country.",
        "hs": [("280429", "Rare gases other than argon (shared code)")],
        "fred": None, "yahoo": None,
    },
    "argon": {
        "name": "Argon", "symbol": "Ar", "category": "Industrial gases",
        "sourcing": "Co-product (air separation)", "sourcing_note": "Cryogenic air separation.",
        "uses": "Sputtering, inert atmosphere, welding, fab process.",
        "usgs": {},
        "prod_note": "No per-country production data: argon is separated from air wherever "
                     "air-separation plants run; supply tracks industrial-gas capacity.",
        "hs": [("280421", "Argon")],
        "fred": None, "yahoo": None,
    },
    "nitrogen": {
        "name": "Nitrogen (fixed, as ammonia)", "symbol": "N", "category": "Industrial gases",
        "sourcing": "Primary (abundant)", "sourcing_note": "Air separation; fixed nitrogen tracked via ammonia plants.",
        "uses": "Clean-air switchgear medium, NF₃ feedstock, process inerting; fertilisers.",
        "usgs": {
            "production": {"label": "Ammonia plant production (N content)",
                           "mcs2026": ("Nitrogen (Fixed)—Ammonia", "Plant production"),
                           "mcs2025": ("Nitrogen(fixed) - Ammonia", "Plant production")},
        },
        "hs": [("2814", "Ammonia")],
        "fred": None, "yahoo": None,
    },
}
