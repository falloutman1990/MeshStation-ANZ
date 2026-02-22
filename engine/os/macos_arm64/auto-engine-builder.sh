#!/usr/bin/env bash
#######################################
### macOS ENGINE BUILDER By IronGiu ###
#######################################
set -euo pipefail

# ---- Config ----
ENV_PATH="./runtime"
MM="./micromamba"

# ---- Keep list (relative to ENV root) ----
KEEP_FILES=(
  "lib/libFLAC.14.dylib"
  "lib/libSoapySDR.0.8.dylib"
  "lib/libairspy.0.dylib"
  "lib/libairspyhf.0.dylib"
  "lib/libbladeRF.2.dylib"
  "lib/libboost_chrono.dylib"
  "lib/libboost_filesystem.dylib"
  "lib/libboost_program_options.dylib"
  "lib/libboost_serialization.dylib"
  "lib/libboost_system.dylib"
  "lib/libboost_thread.dylib"
  "lib/libcrypto.3.dylib"
  "lib/libfftw3f.3.dylib"
  "lib/libfftw3f_threads.3.dylib"
  "lib/libgcc_s.1.1.dylib"
  "lib/libgfortran.5.dylib"
  "lib/libgnuradio-iqbalance.3.9.0.dylib"
  "lib/libgnuradio-uhd.3.10.12.dylib"
  "lib/libhackrf.0.dylib"
  "lib/libmirisdr.4.dylib"
  "lib/libmp3lame.0.dylib"
  "lib/libmpg123.0.dylib"
  "lib/libogg.0.dylib"
  "lib/libomp.dylib"
  "lib/liborc-0.4.0.dylib"
  "lib/libosmodsp.0.dylib"
  "lib/libquadmath.0.dylib"
  "lib/librtlsdr.0.dylib"
  "lib/libsndfile.1.dylib"
  "lib/libopus.0.dylib"
  "lib/libssl.3.dylib"
  "lib/libthrift.0.22.0.dylib"
  "lib/libuhd.4.9.0.dylib"
  "lib/libusb-1.0.0.dylib"
  "lib/libvolk.3.2.dylib"
  "lib/libvorbis.0.4.9.dylib"
  "lib/libvorbisenc.2.0.12.dylib"
  "lib/libbz2.dylib"
  "lib/libffi.8.dylib"
  "lib/liblzma.5.dylib"
  "lib/libz.1.dylib"

  # Python extensions (.so) observed
  "lib/python3.10/lib-dynload/_bz2.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_contextvars.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_csv.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_ctypes.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_datetime.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_decimal.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_heapq.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_json.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_lzma.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_pickle.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_posixsubprocess.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_queue.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_socket.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/_struct.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/array.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/cmath.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/fcntl.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/math.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/select.cpython-310-darwin.so"
  "lib/python3.10/lib-dynload/zlib.cpython-310-darwin.so"

  # Package-specific python extensions observed
  "lib/python3.10/site-packages/gnuradio/blocks/blocks_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/gnuradio/fft/fft_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/gnuradio/filter/filter_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/gnuradio/gr/gr_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/gnuradio/lora_sdr/lora_sdr_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/gnuradio/network/network_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/gnuradio/pdu/pdu_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/numpy/_core/_multiarray_umath.cpython-310-darwin.so"
  "lib/python3.10/site-packages/numpy/linalg/_umath_linalg.cpython-310-darwin.so"
  "lib/python3.10/site-packages/osmosdr/osmosdr_python.cpython-310-darwin.so"
  "lib/python3.10/site-packages/pmt/pmt_python.cpython-310-darwin.so"

  # Linked dylibs observed via site-packages relative paths
  "lib/libgnuradio-blocks.3.10.12.dylib"
  "lib/libgnuradio-fft.3.10.12.dylib"
  "lib/libgnuradio-filter.3.10.12.dylib"
  "lib/libfmt.12.dylib"
  "lib/libgmp.10.dylib"
  "lib/libgnuradio-runtime.3.10.12.dylib"
  "lib/libspdlog.1.16.dylib"
  "lib/libgnuradio-lora_sdr.1.0.0git.dylib"
  "lib/libgnuradio-network.3.10.12.dylib"
  "lib/libgnuradio-pdu.3.10.12.dylib"
  "lib/libgnuradio-osmosdr.0.2.0.dylib"
  "lib/libc++.1.dylib"
  "lib/libgnuradio-pmt.3.10.12.dylib"
  "lib/libcblas.3.dylib"
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
  if [[ "$arch" == "arm64" ]]; then
    mm_asset="micromamba-osx-arm64"
  else
    mm_asset="micromamba-osx-64"
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
  rm -rf "${ENV_PATH}/share/doc" 2>/dev/null || true
  rm -rf "${ENV_PATH}/share/man" 2>/dev/null || true
  rm -rf "${ENV_PATH}/share/gtk-doc" 2>/dev/null || true
  rm -rf "${ENV_PATH}/plugins" 2>/dev/null || true
  rm -rf "${ENV_PATH}/translations" 2>/dev/null || true
  rm -rf "${ENV_PATH}/conda-meta" 2>/dev/null || true
  find "$ENV_PATH" -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
  find "$ENV_PATH" -type f \( -name "*.a" -o -name "*.la" \) -delete 2>/dev/null || true
}

# Build a set of absolute paths to keep (including symlink targets if present).
realpath_fallback() {
  # Resolve symlinks to an absolute path (macOS-safe).
  # Usage: realpath_fallback "/path/to/file"
  local p="$1"
  # If it's not a symlink, just print the original.
  if [[ ! -L "$p" ]]; then
    printf '%s\n' "$p"
    return
  fi

  # Try readlink (mac provides readlink without -f, so we resolve relative target).
  local target
  target="$(readlink "$p" || true)"
  if [[ -n "$target" ]]; then
    if [[ "$target" == /* ]]; then
      printf '%s\n' "$target"
    else
      # Relative symlink target: resolve against parent dir.
      local dir
      dir="$(cd "$(dirname "$p")" && pwd)"
      printf '%s\n' "${dir}/${target}"
    fi
    return
  fi

  # Final fallback: python (inside env to avoid system python if not present).
  "$MM" run -p "$ENV_PATH" python3 -c "import os,sys; print(os.path.realpath(sys.argv[1]))" "$p"
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
        real="$(realpath_fallback "$abs" || true)"
        [[ -n "$real" && -e "$real" ]] && printf '%s\n' "$real" >> "$out"
      fi
    fi
  done

  sort -u "$out" -o "$out"
}

auto_add_dylib_versions() {
  local keepfile="$1"
  
  # For each .dylib symlink in keep list, add the real target if it exists
  while IFS= read -r rel; do
    [[ "$rel" == lib/*.dylib ]] || continue
    
    local abs="${ENV_PATH}/${rel}"
    [[ -L "$abs" ]] || continue
    
    local target
    target="$(readlink "$abs" 2>/dev/null || true)"
    [[ -n "$target" ]] || continue
    
    # If target is relative, build absolute path
    local target_abs
    if [[ "$target" == /* ]]; then
      target_abs="$target"
    else
      target_abs="${ENV_PATH}/lib/${target}"
    fi
    
    [[ -e "$target_abs" ]] && printf '%s\n' "$target_abs" >> "$keepfile"
  done < <(printf '%s\n' "${KEEP_FILES[@]}")
  
  sort -u "$keepfile" -o "$keepfile"
}

in_keep() {
  # Check if absolute path is in keep list file.
  local file="$1"
  local keepfile="$2"
  grep -Fxq "$file" "$keepfile"
}

prune_env_lib_dylibs() {
  local keepfile="$1"
  [[ -d "${ENV_PATH}/lib" ]] || return

  while IFS= read -r f; do
    # Skip symlinks, keep only regular files
    [[ -L "$f" ]] && continue
    
    if ! in_keep "$f" "$keepfile"; then
      rm -f "$f" || true
    fi
  done < <(find "${ENV_PATH}/lib" -maxdepth 1 -type f -name "*.dylib" 2>/dev/null)
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
This file will create a micro-env for the program's internal radio, 
it will use micromamba for the creation, if there is no micromamba in the script
folder, it will be downloaded, after the env is created, some fixes will be applied 
to reduce its size and a cleanup will be done.
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
  echo "3) Abort and exit."
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

keep_abs="$(mktemp)"
build_keep_abs_file "$keep_abs"
auto_add_dylib_versions "$keep_abs"

echo "Pruning ENV/lib dylibs..."
prune_env_lib_dylibs "$keep_abs"

echo "Pruning Python .so..."
prune_python_sos "$keep_abs"

echo "Re-testing after pruning..."
test_env

echo "Cleaning micromamba cache..."
clean_micromamba_cache

echo "Done."

echo "Done, now you can use the internal radio of the app."
