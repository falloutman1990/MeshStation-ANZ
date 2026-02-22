#!/usr/bin/env bash
#######################################
### LINUX ENGINE BUILDER By IronGiu ###
#######################################
set -euo pipefail

# ---- Config ----
ENV_PATH="./runtime"
MM="./micromamba"

# ---- Keep list (relative to ENV root) ----
KEEP_FILES=(
  # System libraries (libc, libm, etc. - usually system-provided, but keep references)
  "lib/libc.so.6"
  "lib/libdl.so.2"
  "lib/libm.so.6"
  "lib/libpthread.so.0"
  "lib/librt.so.1"
  "lib/libutil.so.1"
  
  # Python core libraries
  "lib/libbz2.so.1.0"
  "lib/libffi.so.8"
  "lib/liblzma.so.5"
  "lib/libz.so.1"
  
  # Python standard library extensions
  "lib/python3.10/lib-dynload/array.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_bz2.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/cmath.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_contextvars.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_csv.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_ctypes.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_datetime.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_decimal.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/fcntl.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_heapq.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_json.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_lzma.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/math.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_pickle.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_posixsubprocess.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_queue.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/select.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_socket.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/_struct.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/lib-dynload/zlib.cpython-310-aarch64-linux-gnu.so"
  
  # GNU Radio Python bindings
  "lib/python3.10/site-packages/gnuradio/blocks/blocks_python.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/gnuradio/fft/fft_python.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/gnuradio/filter/filter_python.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/gnuradio/gr/gr_python.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/gnuradio/lora_sdr/lora_sdr_python.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/gnuradio/network/network_python.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/gnuradio/pdu/pdu_python.cpython-310-aarch64-linux-gnu.so"
  
  # NumPy extensions
  "lib/python3.10/site-packages/numpy/_core/_multiarray_umath.cpython-310-aarch64-linux-gnu.so"
  "lib/python3.10/site-packages/numpy/linalg/_umath_linalg.cpython-310-aarch64-linux-gnu.so"
  
  # OsmoSDR Python bindings
  "lib/python3.10/site-packages/osmosdr/osmosdr_python.cpython-310-aarch64-linux-gnu.so"
  
  # PMT Python bindings
  "lib/python3.10/site-packages/pmt/pmt_python.cpython-310-aarch64-linux-gnu.so"
  
  # Audio libraries
  "lib/libFLAC.so.14"
  "lib/libsndfile.so.1"
  "lib/libmp3lame.so.0"
  "lib/libmpg123.so.0"
  "lib/libogg.so.0"
  "lib/libopus.so.0"
  "lib/libvorbis.so.0.4.9"
  "lib/libvorbisenc.so.2.0.12"
  
  # GNU Radio core libraries
  "lib/libgnuradio-blocks.so.3.10.12"
  "lib/libgnuradio-fft.so.3.10.12"
  "lib/libgnuradio-filter.so.3.10.12"
  "lib/libgnuradio-runtime.so.3.10.12"
  "lib/libgnuradio-lora_sdr.so.1.0.0git"
  "lib/libgnuradio-network.so.3.10.12"
  "lib/libgnuradio-pdu.so.3.10.12"
  "lib/libgnuradio-pmt.so.3.10.12"
  "lib/libgnuradio-audio.so.3.10.12"
  "lib/libgnuradio-funcube.so.3.10.0"
  "lib/libgnuradio-iqbalance.so.3.9.0"
  "lib/libgnuradio-uhd.so.3.10.12"
  "lib/libgnuradio-osmosdr.so.0.2.0"
  
  # FFT libraries
  "lib/libfftw3f.so.3"
  "lib/libfftw3f_threads.so.3"
  
  # Boost libraries
  "lib/libboost_thread.so.1.88.0"
  "lib/libboost_program_options.so.1.88.0"
  "lib/libboost_chrono.so.1.88.0"
  "lib/libboost_filesystem.so.1.88.0"
  "lib/libboost_serialization.so.1.88.0"
  "lib/libboost_system.so.1.88.0"
  
  # Crypto/SSL
  "lib/libcrypto.so.3"
  "lib/libssl.so.3"
  
  # Misc libraries
  "lib/libfmt.so.12"
  "lib/libgmp.so.10"
  "lib/libspdlog.so.1.16"
  "lib/libthrift.so.0.22.0"
  "lib/libunwind.so.8"
  
  # Math libraries
  "lib/libcblas.so.3"
  "lib/libblas.so.3"
  "lib/liblapack.so.3"
  "lib/libgfortran.so.5"
  "lib/libquadmath.so.0"
  
  # SDR hardware libraries
  "lib/libairspy.so.0"
  "lib/libairspyhf.so.0"
  "lib/libbladeRF.so.2"
  "lib/libhackrf.so.0"
  "lib/libmirisdr.so.4"
  "lib/librtlsdr.so.0"
  "lib/libSoapySDR.so.0.8"
  "lib/libuhd.so.4.9.0"
  
  # System dependencies
  "lib/libasound.so.2"
  "lib/libcap.so.2"
  "lib/libhidapi-libusb.so.0"
  "lib/libjack.so.0"
  "lib/libosmodsp.so.0"
  "lib/libportaudio.so"
  "lib/libudev.so.1"
  "lib/libusb-1.0.so.0"
  
  # C++ runtime and utils
  "lib/libgcc_s.so.1"
  "lib/libstdc++.so.6"
  "lib/libstdc++.so"  "
  lib/libstdc++.so.6"
  "lib/libstdc++.so.6.0.34"
  "lib/liborc-0.4.so.0"
  "lib/libvolk.so.3.2"

  # GNU Radio core libraries (full versions - actual files)
  "lib/libgnuradio-analog.so.3.10.12.0"
  "lib/libgnuradio-audio.so.3.10.12.0"
  "lib/libgnuradio-blocks.so.3.10.12.0"
  "lib/libgnuradio-channels.so.3.10.12.0"
  "lib/libgnuradio-digital.so.3.10.12.0"
  "lib/libgnuradio-dtv.so.3.10.12.0"
  "lib/libgnuradio-fec.so.3.10.12.0"
  "lib/libgnuradio-fft.so.3.10.12.0"
  "lib/libgnuradio-filter.so.3.10.12.0"
  "lib/libgnuradio-funcube.so.3.10.0.0"
  "lib/libgnuradio-iqbalance.so.3.9.0.0"
  "lib/libgnuradio-network.so.3.10.12.0"
  "lib/libgnuradio-osmosdr.so.0.2.0.0"
  "lib/libgnuradio-pdu.so.3.10.12.0"
  "lib/libgnuradio-pmt.so.3.10.12.0"
  "lib/libgnuradio-runtime.so.3.10.12.0"
  "lib/libgnuradio-trellis.so.3.10.12.0"
  "lib/libgnuradio-uhd.so.3.10.12.0"
  "lib/libgnuradio-vocoder.so.3.10.12.0"
  "lib/libgnuradio-wavelet.so.3.10.12.0"
  
  # Others
  "lib/libvolk.so.3.2.0"
  "/lib/libvolk.so"
)

# ---- Helpers ----
confirm() {
  local prompt="$1"
  read -r -p "$prompt" ans
  [[ "$(printf '%s' "$ans" | tr '[:upper:]' '[:lower:]')" == "ok" ]]
}

download_micromamba_if_needed() {
  if [[ -x "$MM" ]]; then
    echo "micromamba already exists, proceeding..."
    return
  fi

  echo "Downloading micromamba..."
  local arch mm_asset
  arch="$(uname -m)"
  
  if [[ "$arch" == "x86_64" ]]; then
    mm_asset="micromamba-linux-64"
  elif [[ "$arch" == "aarch64" ]]; then
    mm_asset="micromamba-linux-aarch64"
  else
    echo "Unsupported architecture: $arch"
    exit 1
  fi

  curl -fLsS "https://github.com/mamba-org/micromamba-releases/releases/latest/download/${mm_asset}" -o "$MM"
  chmod +x "$MM"
  echo "micromamba downloaded and made executable."
}

build_env() {
  echo "Building the environment..."
  "$MM" create -p "$ENV_PATH" -c conda-forge -c ryanvolz \
    nomkl \
    python=3.10 \
    gnuradio-osmosdr \
    gnuradio-lora_sdr \
    rtl-sdr \
    libusb \
    numpy \
    --yes
}

test_env() {
  echo "Testing the environment..."
  "$MM" run -p "$ENV_PATH" python -c "import gnuradio, pmt, osmosdr, numpy; import gnuradio.lora_sdr as l; print('OK')"
}

safe_cleanup() {
  echo "Safe cleanup (docs/includes/cache/etc)..."
  rm -rf "${ENV_PATH}/include" 2>/dev/null || true
  rm -rf "${ENV_PATH}/plugins" 2>/dev/null || true
  rm -rf "${ENV_PATH}/translations" 2>/dev/null || true
  rm -rf "${ENV_PATH}/conda-meta" 2>/dev/null || true
  rm -rf "${ENV_PATH}/share" 2>/dev/null || true
  rm -rf "${ENV_PATH}/ssl" 2>/dev/null || true
  rm -rf "${ENV_PATH}/etc" 2>/dev/null || true
  rm -rf "${ENV_PATH}/pkgconfig" 2>/dev/null || true
  rm -rf "${ENV_PATH}/cmake" 2>/dev/null || true
  # Delete test and examples
  find "$ENV_PATH" -type d -name "tests" -prune -exec rm -rf {} + 2>/dev/null || true
  find "$ENV_PATH" -type d -name "test" -prune -exec rm -rf {} + 2>/dev/null || true
  find "$ENV_PATH" -type d -name "examples" -prune -exec rm -rf {} + 2>/dev/null || true
  # Remove bytecode optimized files (keep only .pyc)
  find "$ENV_PATH" -name "*.pyo" -delete 2>/dev/null || true
  find "$ENV_PATH" -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
  find "$ENV_PATH" -type f \( -name "*.a" -o -name "*.la" \) -delete 2>/dev/null || true
}

strip_binaries() {
  echo "Stripping debug symbols from binaries..."
  find "${ENV_PATH}/lib" -type f -name "*.so*" ! -name "*.py" -exec strip --strip-debug {} \; 2>/dev/null || true
  find "${ENV_PATH}/bin" -type f -executable -exec strip --strip-unneeded {} \; 2>/dev/null || true
}

build_keep_abs_file() {
  local out="$1"
  : > "$out"

  for rel in "${KEEP_FILES[@]}"; do
    local abs="${ENV_PATH}/${rel}"
    if [[ -e "$abs" ]]; then
      printf '%s\n' "$abs" >> "$out"
      if [[ -L "$abs" ]]; then
        local real
        real="$(readlink -f "$abs" 2>/dev/null || true)"
        [[ -n "$real" && -e "$real" ]] && printf '%s\n' "$real" >> "$out"
      fi
    fi
  done

  sort -u "$out" -o "$out"
}

auto_add_so_versions() {
  local keepfile="$1"
  
  for rel in "${KEEP_FILES[@]}"; do
    [[ "$rel" == lib/*.so* ]] || continue
    
    local abs="${ENV_PATH}/${rel}"
    [[ -e "$abs" ]] || continue
    
    # add file/symlink itself
    printf '%s\n' "$abs" >> "$keepfile"
    
    # If it's a symlink, follow the chain until the real file
    local current="$abs"
    while [[ -L "$current" ]]; do
      local target
      target="$(readlink "$current" 2>/dev/null || true)"
      [[ -z "$target" ]] && break
      
      # If relative, resolve relative to symlink directory
      if [[ "$target" != /* ]]; then
        target="$(dirname "$current")/$target"
      fi
      
      [[ -e "$target" ]] && printf '%s\n' "$target" >> "$keepfile"
      current="$target"
    done
    
    # If it's not a symlink, check for versioned files (fallback)
    local pattern="${abs}*"
    while IFS= read -r versioned; do
      printf '%s\n' "$versioned" >> "$keepfile"
    done < <(compgen -G "$pattern" 2>/dev/null || true)
  done
  
  sort -u "$keepfile" -o "$keepfile"
}

in_keep() {
  local file="$1"
  local keepfile="$2"
  grep -Fxq "$file" "$keepfile"
}

prune_env_lib_sos() {
  local keepfile="$1"
  [[ -d "${ENV_PATH}/lib" ]] || return

  while IFS= read -r f; do
    # Skip symlinks
    [[ -L "$f" ]] && continue
    
    if ! in_keep "$f" "$keepfile"; then
      rm -f "$f" || true
    fi
  done < <(find "${ENV_PATH}/lib" -maxdepth 1 -type f -name "*.so*" 2>/dev/null)
}

prune_python_sos() {
  local keepfile="$1"

  local d1="${ENV_PATH}/lib/python3.10/lib-dynload"
  local d2="${ENV_PATH}/lib/python3.10/site-packages"

  if [[ -d "$d1" ]]; then
    while IFS= read -r f; do
      if ! in_keep "$f" "$keepfile"; then
        rm -f "$f" || true
      fi
    done < <(find "$d1" -type f -name "*.so" 2>/dev/null)
  fi

  if [[ -d "$d2" ]]; then
    while IFS= read -r f; do
      if ! in_keep "$f" "$keepfile"; then
        rm -f "$f" || true
      fi
    done < <(find "$d2" -type f -name "*.so" 2>/dev/null)
  fi
}

clean_micromamba_cache() {
  "$MM" clean --all --yes
}

# ---- Main ----
cat <<'EOF'
This script will create a micro-env for the program's internal radio.
It uses micromamba for creation. If micromamba is not in the script
folder, it will be downloaded. After env creation, cleanup will be applied.

---
If you are ready and connected to the internet, type 'ok', otherwise close this script.
EOF

if ! confirm "Type 'ok' to continue: "; then
  echo "Aborted."
  exit 0
fi

download_micromamba_if_needed

echo "--- Creating environment ---"
if [[ -d "$ENV_PATH" ]]; then
  echo "Environment already exists."
  echo "1) Delete and recreate"
  echo "2) Skip creation and run cleanup only"
  echo "3) Abort and exit"
  read -r -p "Choose (1, 2 or 3): " choice
  
  if [[ "$choice" == "1" ]]; then
    rm -rf "$ENV_PATH" 2>/dev/null || true
    build_env
    test_env
  elif [[ "$choice" == "2" ]]; then
    echo "Skipping creation, proceeding to cleanup..."
  elif [[ "$choice" == "3" ]]; then
    echo "Aborted."
    exit 0
  else
    echo "Invalid choice. Aborted."
    exit 1
  fi
else
  build_env
  test_env
fi

echo "--- Cleanup ---"

safe_cleanup
strip_binaries

keep_abs="$(mktemp)"
build_keep_abs_file "$keep_abs"
auto_add_so_versions "$keep_abs"

echo "Pruning ENV/lib .so files..."
prune_env_lib_sos "$keep_abs"

echo "Pruning Python .so..."
prune_python_sos "$keep_abs"

echo "Re-testing after pruning..."
test_env

echo "Cleaning micromamba cache..."
clean_micromamba_cache

echo "Done. Runtime environment ready at: $ENV_PATH"