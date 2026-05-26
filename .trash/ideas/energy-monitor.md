# Satellite Data Sources for Monitoring China's Economy

## Highest-Frequency Sources

### 1. SpaceKnow — China Satellite Manufacturing Index (SMI)
- **Frequency:** 3x/week (Mon, Wed, Fri)
- **Method:** SAR imagery across 6,000+ industrial areas in China; ML classifies activity levels from buildings, roads, construction
- **Upsides:** Highest-frequency structured economic index available; on Bloomberg terminals; sees through clouds (SAR-based); independent of official Chinese statistics; long track record (since 2014)
- **Downsides:** Commercial/expensive; proprietary methodology (black box); focused on manufacturing — limited coverage of services, consumption, or agriculture

### 2. Planet Labs — Daily Optical Imagery
- **Frequency:** Near-daily (3.7m resolution globally)
- **Method:** ~200 Dove satellites capturing optical imagery of Earth's full landmass every day
- **Upsides:** Highest temporal resolution of any optical provider; massive 6+ PB archive; analytic feeds with object detection and change indexing; acquired Sentinel Hub for SAR integration
- **Downsides:** Optical = blocked by clouds (major issue for southern/coastal China); raw imagery requires your own ML pipeline to extract economic signals; expensive at scale

### 3. NPP-VIIRS Nighttime Lights
- **Frequency:** Monthly composites (daily raw DNB data available but noisy)
- **Method:** Suomi NPP satellite measures nighttime radiance; proxies for industrial activity, urbanization, electricity consumption
- **Upsides:** **Free** (Creative Commons); long time series back to 2012 (harmonized to 1992 with DMSP); well-studied in academic literature (R-squared of 0.85-0.96 vs GDP at various levels); available via [EOG/Colorado Mines](https://eogdata.mines.edu/products/vnl/) and Google Earth Engine
- **Downsides:** Monthly is the practical usable frequency; light saturation in dense urban cores; confounded by policy (e.g., LED adoption, light pollution regulations); doesn't distinguish economic sectors

## Other Notable Sources

| Source | Freq | Type | Key Trade-off |
|---|---|---|---|
| **Orbital Insight** (now Privateer Space) | Weekly+ | Oil storage, industrial activity, traffic | Best for commodity-specific signals (oil tank shadow analysis); less general-purpose |
| **Sentinel-1 SAR** (ESA/Copernicus) | 6-12 day revisit | C-band SAR | **Free**; all-weather; good for infrastructure/port monitoring; but lower temporal freq than commercial |
| **Maxar WorldView** | Tasked (not systematic) | 30cm optical | Highest spatial resolution; great for specific site monitoring; very expensive; not designed for systematic economic indexing |
| **Jilin-1** (Chinese constellation) | Sub-daily capable (117+ sats) | Optical + video | Growing capability; potential access issues for non-Chinese users; less proven analytics ecosystem |

## Practical Recommendations for CMM

If building this into a macro monitor:

1. **Free + academic-grade:** NPP-VIIRS monthly nightlights are the workhorse. Pair with Sentinel-1 SAR for port/industrial zone activity. Both are free and well-documented.
2. **Commercial high-frequency:** SpaceKnow's SMI is the most directly useful pre-built index — 3x/week, China-specific, already structured as a time series. Bloomberg distributes it, so it may be accessible through a terminal.
3. **Build-your-own:** Planet daily imagery + your own CV/ML pipeline gives the most flexibility but the highest engineering cost.

## Key Gap

Satellite data captures **physical** economic activity (manufacturing, construction, logistics, lights) but is weak on **services**, **financial activity**, and **consumption** — which are an increasingly large share of China's GDP.

---

## AI-Powered Energy Infrastructure Detection from Satellite Imagery

### Foundation Models & Toolkits

#### TerraTorch — IBM's Geospatial Foundation Model Toolkit
The most capable unified toolkit for fine-tuning geospatial foundation models (Clay, Prithvi, SatMAE, Satlas, DOFA, DeCur) on downstream tasks like object detection and segmentation. Built on PyTorch.
- [github.com/terrastackai/terratorch](https://github.com/terrastackai/terratorch)

#### TorchGeo 0.9+
PyTorch domain library for geospatial data. Provides datasets, samplers, transforms, and **Earth Embeddings** — pre-computed representations from foundation models that enable rapid analysis without GPU-heavy inference.
- [torchgeo.readthedocs.io](https://torchgeo.readthedocs.io/)

#### segment-geospatial
Applies Meta's **Segment Anything Model (SAM)** to geospatial data. Powerful for zero-shot or few-shot segmentation of infrastructure from satellite imagery.

#### Google Remote Sensing Foundation Models (2025)
Based on SigLIP, MaMMUT, and OWL-ViT adapted for remote sensing. Trained on high-res satellite/aerial images with text descriptions and bounding box annotations — enabling text-prompted detection (e.g., "find solar farms").
- [Google Research blog](https://research.google/blog/geospatial-reasoning-unlocking-insights-with-generative-ai-and-multiple-foundation-models/)

### Turnkey Energy Infrastructure Detection

#### Microsoft Global Renewables Watch (open source, MIT license)
Microsoft + Planet Labs trained deep learning segmentation models that detected **375,197 wind turbines** and **86,410 solar PV installations** globally from high-res satellite imagery (13+ trillion pixels). Pre-trained models and inference scripts included. Quarterly temporal data from Q4 2017 to Q2 2024.
- [github.com/microsoft/global-renewables-watch](https://github.com/microsoft/global-renewables-watch)

#### Power Infrastructure Detection (2025 paper)
Faster R-CNN with ResNet-101 backbone for detecting **power towers, poles, substations, and transmission lines** from satellite imagery (AP50: 60.6%, F1: 74.7%, accuracy: 90.9%).
- [tandfonline.com — power infrastructure detection](https://www.tandfonline.com/doi/full/10.1080/20964471.2025.2490408)

### Object Detection Frameworks

| Framework | Best for | Notes |
|---|---|---|
| **YOLOv8 / YOLOv9** (Ultralytics) | Real-time detection of turbines, panels, plants | Current SOTA for speed/accuracy tradeoff |
| **Faster R-CNN** (torchvision / Detectron2) | High-accuracy infrastructure detection | Better for smaller objects like towers/poles |
| **MMDetection** | Flexible experimentation | Large model zoo, good for benchmarking |

### Core Geospatial Python Stack

| Package | Role |
|---|---|
| `rasterio` | Read/write raster data (GeoTIFF, etc.) |
| `geopandas` | Vector data manipulation |
| `leafmap` | Interactive mapping & visualization |
| `pystac-client` | Access satellite imagery catalogs (Sentinel, Landsat) |
| `planetary-computer` | Microsoft's data catalog (pairs well with GRW) |
| `geoai` | Unified interface for geospatial AI tasks |

### Detectable Energy Infrastructure Types

- **Solar farms** — GRW pre-trained models; also CNNs + SVMs on Sentinel-2 multispectral
- **Wind turbines** — GRW pre-trained models; also YOLOv7/v8 on LANDSAT/NAIP
- **Transmission lines & towers** — Faster R-CNN; ArcGIS `arcgis.learn` with RetinaNet
- **Substations** — Faster R-CNN with ResNet backbone
- **Thermal/nuclear power plants** — Part-Based Context Attention Networks
- **Battery storage systems** — No pre-trained models yet; would require fine-tuning a foundation model on labeled data

### Recommended Approach for CMM

1. **Start with Global Renewables Watch** — pre-trained models for solar/wind, MIT licensed, adapt for China
2. **Use TerraTorch + TorchGeo** to fine-tune a foundation model for infrastructure GRW doesn't cover (nuclear plants, battery storage, transmission lines)
3. **Data sources**: Sentinel-2 (free, 10m resolution) via `pystac-client`, or Planet Labs (commercial, higher res)
4. **Detection model**: YOLOv8/v9 for larger structures (plants, farms), Faster R-CNN for smaller infrastructure (towers, lines)

### Reference Book

**"GeoAI with Python" by Qiusheng Wu (2026)** — covers the full pipeline from downloading satellite data to training deep learning models using TorchGeo, segment-geospatial, leafmap, and geoai.
- [book.opengeoai.org](https://book.opengeoai.org/)

---

## Sources
- [SpaceKnow Economic Nowcasting](https://spaceknow.com/products/economic/)
- [SpaceKnow China Case Study](https://spaceknow.com/case-studies/china/)
- [EOG VIIRS Nighttime Light Products](https://eogdata.mines.edu/products/vnl/)
- [World Bank — Measuring Growth from Outer Space](https://blogs.worldbank.org/en/developmenttalk/measuring-quarterly-economic-growth-outer-space)
- [Prolonged Artificial Nighttime-light Dataset of China (1984-2020)](https://www.nature.com/articles/s41597-024-03223-1)
- [Orbital Insight / Alternative Data](https://alternativedata.org/data_provider/orbital-insight/)
- [Planet Monitoring](https://www.planet.com/products/satellite-monitoring/)
- [Sentinel-1 at NASA Earthdata](https://www.earthdata.nasa.gov/data/platforms/space-based-platforms/sentinel-1)
- [CSIS — China GDP Proxies](https://bigdatachina.csis.org/measurement-muddle-chinas-gdp-growth-data-and-potential-proxies/)
- [TerraTorch — Geospatial Foundation Models Toolkit](https://github.com/terrastackai/terratorch)
- [TorchGeo Documentation](https://torchgeo.readthedocs.io/)
- [Microsoft Global Renewables Watch](https://github.com/microsoft/global-renewables-watch)
- [Google Geospatial Reasoning](https://research.google/blog/geospatial-reasoning-unlocking-insights-with-generative-ai-and-multiple-foundation-models/)
- [GeoAI with Python (book)](https://book.opengeoai.org/)
- [Power Infrastructure Detection (2025)](https://www.tandfonline.com/doi/full/10.1080/20964471.2025.2490408)
- [Satellite Image Deep Learning Techniques](https://github.com/satellite-image-deep-learning/techniques)
- [Global Renewables Watch Paper (arXiv)](https://arxiv.org/html/2503.14860v1)
- [Earth AI Foundation Models (arXiv)](https://arxiv.org/html/2510.18318v2)