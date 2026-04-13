# ══════════════════════════════════════════════════════════════════════════════
# BIRDY-EDWARDS — Dockerfile
# Base: Ubuntu 24.04 LTS
# Python: 3.12
# Ollama: runs on HOST, called via host.docker.internal:11434
# Cookie Refresh: Cookie-Editor import UI only (no X11/Selenium refresh)
# ══════════════════════════════════════════════════════════════════════════════

FROM ubuntu:24.04

# Prevent interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# ── Labels ──
LABEL maintainer="Jeet Ganguly"
LABEL description="BIRDY-EDWARDS Facebook SOCMINT Platform"
LABEL version="1.0"

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — System packages
# ══════════════════════════════════════════════════════════════════════════════
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Python
    python3.12 \
    python3.12-dev \
    python3-pip \
    python3.12-venv \
    # Build tools (required for dlib)
    build-essential \
    cmake \
    g++ \
    make \
    pkg-config \
    # dlib dependencies
    libopenblas-dev \
    liblapack-dev \
    libboost-all-dev \
    libx11-dev \
    libgtk-3-dev \
    libboost-python-dev \
    libboost-thread-dev \
    # Image processing
    libpng-dev \
    libjpeg-dev \
    libtiff-dev \
    libwebp-dev \
    libopencv-dev \
    # Tesseract OCR
    tesseract-ocr \
    tesseract-ocr-ben \
    tesseract-ocr-hin \
    tesseract-ocr-ara \
    tesseract-ocr-urd \
    tesseract-ocr-eng \
    # Virtual display (for SeleniumBase scraping)
    xvfb \
    x11-utils \
    # Chrome dependencies
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libasound2t64 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    # Utilities
    wget \
    curl \
    unzip \
    git \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — Google Chrome stable
# ══════════════════════════════════════════════════════════════════════════════
RUN wget -q -O /tmp/chrome.deb \
    https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — Create virtualenv + install all packages
# ══════════════════════════════════════════════════════════════════════════════
RUN python3.12 -m venv /app/venv

ENV PATH="/app/venv/bin:$PATH"

RUN pip install --upgrade pip

RUN apt-get update && apt-get install -y python3-tk && rm -rf /var/lib/apt/lists/*
# Install all packages except dlib and face_recognition
RUN pip install \
    flask \
    seleniumbase \
    Pillow \
    requests \
    pytesseract \
    reportlab \
    networkx \
    pyvis \
    matplotlib \
    seaborn \
    psutil \
    ollama \
    numpy \
    scipy \
    scikit-learn \
    click \
    python-dateutil \
    tqdm \
    opencv-python


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4 — dlib (compiled from source)
# ══════════════════════════════════════════════════════════════════════════════
RUN pip install dlib

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 5 — face_recognition (depends on dlib)
# ══════════════════════════════════════════════════════════════════════════════
RUN pip install face_recognition -q
RUN pip install git+https://github.com/ageitgey/face_recognition_models

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 6 — App setup
# ══════════════════════════════════════════════════════════════════════════════

# ── Patch face_recognition_models for Python 3.12 compatibility ──
RUN python3 - << 'PYEOF'
import sys, os
path = os.path.join(sys.prefix, 'lib', f'python{sys.version_info.major}.{sys.version_info.minor}', 'site-packages')
init = os.path.join(path, 'face_recognition_models', '__init__.py')
if not os.path.exists(init):
    print("face_recognition_models not found — skipping patch")
    exit(0)
content = open(init).read()
if 'pkg_resources' not in content:
    print("Already patched — skipping")
    exit(0)
new_content = '''import os as _os
_here = _os.path.dirname(_os.path.abspath(__file__))
def pose_predictor_model_location():
    return _os.path.join(_here, "models/shape_predictor_68_face_landmarks.dat")
def pose_predictor_five_point_model_location():
    return _os.path.join(_here, "models/shape_predictor_5_face_landmarks.dat")
def face_recognition_model_location():
    return _os.path.join(_here, "models/dlib_face_recognition_resnet_model_v1.dat")
def cnn_face_detector_model_location():
    return _os.path.join(_here, "models/mmod_human_face_detector.dat")
'''
open(init, 'w').write(new_content)
print("Patched:", init)
PYEOF
# Create app directory
WORKDIR /app

# Create required runtime directories
RUN mkdir -p \
    /app/reports \
    /app/face_data \
    /app/post_screenshots \
    /app/status \
    /app/icons

# Copy application code
# (this layer rebuilds on every code change — fast since all deps already cached)
COPY app/ /app/

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# ── Ollama host (calls host machine's Ollama instance) ──
ENV OLLAMA_HOST=http://host.docker.internal:11434

# ── Xvfb display for SeleniumBase scraping ──
ENV DISPLAY=:99

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 7 — Entrypoint
# ══════════════════════════════════════════════════════════════════════════════
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/docker-entrypoint.sh"]