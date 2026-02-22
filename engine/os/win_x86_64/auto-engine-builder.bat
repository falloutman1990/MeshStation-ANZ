@echo off
REM #########################################
REM ### WINDOWS ENGINE BUILDER By IronGiu ###
REM #########################################
set ENV_PATH=.\runtime

echo This file will create a micro-env for the program's internal radio, 
echo it will use micromamba for the creation, if there is no micromamba in the script
echo folder, it will be downloaded, after the env is created, some fixes will be applied 
echo to reduce its size and a cleanup will be done.
echo ---
echo If you are ready and connected to the internet, type 'ok', otherwise close this script.

set /p input="Type 'ok' to continue or close the script: "

REM check if the input is 'ok' (case insensitive)
if /i not "%input%"=="ok" (
    echo Input is not 'ok', closing the script...
    pause
    exit
)

REM if ok

echo --- Creating environment ---
if not exist micromamba.exe (
    echo 1. Downloading micromamba...
    curl -Ls https://github.com/mamba-org/micromamba-releases/releases/latest/download/micromamba-win-64 -o micromamba.exe
    if errorlevel 1 (
        echo Download failed! Please download micromamba manually and place it in this folder.
        pause
        exit
    )
    echo 1.1 micromamba downloaded successfully, proceeding...
) else (
    echo 1. micromamba already exists, proceeding...
)

echo 2. Building the environment...
micromamba create -p %ENV_PATH% -c conda-forge -c ryanvolz ^
  nomkl ^
  python=3.10 ^
  gnuradio-osmosdr ^
  gnuradio-lora_sdr ^
  rtl-sdr ^
  libusb ^
  numpy ^
  --yes

echo 3. testing the environment...
%ENV_PATH%\python -c "import gnuradio, pmt, osmosdr, numpy; import gnuradio.lora_sdr as l; print('OK')" || (
    echo 3.1 Test failed, open an issue on GitHub and report the error, closing the script...
    pause
    exit
)

echo Test passed, proceeding with cleanup...

echo --- Cleaning up of the %ENV_PATH% folder ---

echo 1. Remove Headers, uselsess files, folders and Documentation
rd /s /q "%ENV_PATH%\include" 2>nul
rd /s /q "%ENV_PATH%\Library\include" 2>nul
rd /s /q "%ENV_PATH%\Library\plugins" 2>nul
rd /s /q "%ENV_PATH%\Library\translations" 2>nul
rd /s /q "%ENV_PATH%\Lib\site-packages\PyQt5" 2>nul
rd /s /q "%ENV_PATH%\share\doc" 2>nul
rd /s /q "%ENV_PATH%\share\man" 2>nul
rd /s /q "%ENV_PATH%\share\gtk-doc" 2>nul
del /s /q "%ENV_PATH%\Library\bin\assistant.exe" 2>nul
del /s /q "%ENV_PATH%\Library\bin\designer.exe" 2>nul
del /s /q "%ENV_PATH%\Library\bin\linguist.exe" 2>nul
del /s /q "%ENV_PATH%\Library\bin\qmake.exe" 2>nul

echo 2. Remove Conda Metadata (Makes the environment no longer editable via Conda)
rd /s /q "%ENV_PATH%\conda-meta" 2>nul

echo 3. Remove development files (CMake and PkgConfig)
rd /s /q "%ENV_PATH%\Library\lib\cmake" 2>nul
rd /s /q "%ENV_PATH%\Library\lib\pkgconfig" 2>nul
rd /s /q "%ENV_PATH%\lib\cmake" 2>nul
rd /s /q "%ENV_PATH%\lib\pkgconfig" 2>nul

echo 4. Remove static libraries
del /s /q "%ENV_PATH%\*.a" 2>nul
del /s /q "%ENV_PATH%\*.lib" 2>nul

echo 5. Remove temporary Python files
for /d /r "%ENV_PATH%" %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"

echo 6. Keep only required DLLs inside %ENV_PATH%
if exist "%ENV_PATH%" (
    powershell -NoProfile -ExecutionPolicy Bypass ^
      "$envRoot = (Resolve-Path '%ENV_PATH%').Path;" ^
      "$keepRelative = @(" ^
      "    'libusb-1.0.dll'," ^
      "    'DEVOBJ.dll'," ^
      "    'MSASN1.dll'," ^
      "    'python3.dll'," ^
      "    'libwinpthread-1.dll'," ^
      "    'python310.dll'," ^
      "    'zlib.dll'," ^
      "    'msvcp140.dll'," ^
      "    'uhd.dll'," ^
      "    'vcruntime140.dll'," ^
      "    'vcruntime140_1.dll'," ^
      "    'vorbis.dll'," ^
      "    'CRYPTBASE.dll'," ^
      "    'CRYPTSP.dll'," ^
      "    'FLAC.dll'," ^
      "    'IPHLPAPI.dll'," ^
      "    'MSWSOCK.dll'," ^
      "    'SoapySDR.dll'," ^
      "    'VERSION.dll'," ^
      "    'WSOCK32.dll'," ^
      "    'airspy.dll'," ^
      "    'airspyhf.dll'," ^
      "    'bladeRF-2.dll'," ^
      "    'boost_filesystem.dll'," ^
      "    'boost_program_options.dll'," ^
      "    'boost_serialization.dll'," ^
      "    'boost_thread.dll'," ^
      "    'ffi-8.dll'," ^
      "    'fftw3f.dll'," ^
      "    'fmt.dll'," ^
      "    'gnuradio-blocks.dll'," ^
      "    'gnuradio-fft.dll'," ^
      "    'gnuradio-filter.dll'," ^
      "    'gnuradio-iqbalance.dll'," ^
      "    'gnuradio-lora_sdr.dll'," ^
      "    'gnuradio-network.dll'," ^
      "    'gnuradio-osmosdr.dll'," ^
      "    'gnuradio-pdu.dll'," ^
      "    'gnuradio-pmt.dll'," ^
      "    'gnuradio-runtime.dll'," ^
      "    'gnuradio-uhd.dll'," ^
      "    'hackrf-0.dll'," ^
      "    'libblas.dll'," ^
      "    'libbz2.dll'," ^
      "    'libcblas.dll'," ^
      "    'liblapack.dll'," ^
      "    'liblzma.dll'," ^
      "    'libmp3lame.dll'," ^
      "    'libosmodsp-0.dll'," ^
      "    'mirisdr-4.dll'," ^
      "    'mpg123.dll'," ^
      "    'mpir.dll'," ^
      "    'ogg.dll'," ^
      "    'opus.dll'," ^
      "    'rtlsdr.dll'," ^
      "    'sndfile.dll'," ^
      "    'spdlog.dll'," ^
      "    'volk.dll'," ^
      "    'vorbisenc.dll'," ^
      "    'DLLs\IPHLPAPI.dll'," ^
      "    'DLLs\ffi-8.dll'," ^
      "    'DLLs\libbz2.dll'," ^
      "    'DLLs\liblzma.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\FLAC.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\gnuradio-blocks.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\libmp3lame.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\mpg123.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\ogg.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\opus.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\sndfile.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\vorbisenc.dll'," ^
      "    'Lib\site-packages\gnuradio\fft\fftw3f.dll'," ^
      "    'Lib\site-packages\gnuradio\fft\gnuradio-fft.dll'," ^
      "    'Lib\site-packages\gnuradio\filter\gnuradio-filter.dll'," ^
      "    'Lib\site-packages\gnuradio\gr\boost_program_options.dll'," ^
      "    'Lib\site-packages\gnuradio\gr\boost_thread.dll'," ^
      "    'Lib\site-packages\gnuradio\gr\fmt.dll'," ^
      "    'Lib\site-packages\gnuradio\gr\gnuradio-runtime.dll'," ^
      "    'Lib\site-packages\gnuradio\gr\mpir.dll'," ^
      "    'Lib\site-packages\gnuradio\gr\spdlog.dll'," ^
      "    'Lib\site-packages\gnuradio\lora_sdr\gnuradio-lora_sdr.dll'," ^
      "    'Lib\site-packages\gnuradio\network\MSWSOCK.dll'," ^
      "    'Lib\site-packages\gnuradio\network\WSOCK32.dll'," ^
      "    'Lib\site-packages\gnuradio\network\gnuradio-network.dll'," ^
      "    'Lib\site-packages\gnuradio\pdu\gnuradio-pdu.dll'," ^
      "    'Lib\site-packages\numpy\_core\libblas.dll'," ^
      "    'Lib\site-packages\numpy\_core\libcblas.dll'," ^
      "    'Lib\site-packages\numpy\linalg\liblapack.dll'," ^
      "    'Lib\site-packages\osmosdr\SoapySDR.dll'," ^
      "    'Lib\site-packages\osmosdr\airspy.dll'," ^
      "    'Lib\site-packages\osmosdr\airspyhf.dll'," ^
      "    'Lib\site-packages\osmosdr\bladeRF-2.dll'," ^
      "    'Lib\site-packages\osmosdr\boost_filesystem.dll'," ^
      "    'Lib\site-packages\osmosdr\boost_serialization.dll'," ^
      "    'Lib\site-packages\osmosdr\gnuradio-iqbalance.dll'," ^
      "    'Lib\site-packages\osmosdr\gnuradio-osmosdr.dll'," ^
      "    'Lib\site-packages\osmosdr\gnuradio-uhd.dll'," ^
      "    'Lib\site-packages\osmosdr\hackrf-0.dll'," ^
      "    'Lib\site-packages\osmosdr\libosmodsp-0.dll'," ^
      "    'Lib\site-packages\osmosdr\mirisdr-4.dll'," ^
      "    'Lib\site-packages\osmosdr\rtlsdr.dll'," ^
      "    'Lib\site-packages\pmt\MSVCP140.dll'," ^
      "    'Lib\site-packages\pmt\VCRUNTIME140_1.dll'," ^
      "    'Lib\site-packages\pmt\gnuradio-pmt.dll'," ^
      "    'Lib\site-packages\pmt\volk.dll'," ^
      "    'Lib\site-packages\gnuradio\blocks\vorbis.dll'," ^
      "    'Lib\site-packages\osmosdr\uhd.dll'," ^
      "    'Lib\site-packages\osmosdr\libusb-1.0.dll'," ^
      "    'Lib\site-packages\osmosdr\libwinpthread-1.dll'," ^
      "    'Library\bin\IPHLPAPI.dll'," ^
      "    'Library\bin\MSWSOCK.dll'," ^
      "    'Library\bin\WSOCK32.dll'," ^
      "    'Library\bin\ffi-8.dll'," ^
      "    'Library\bin\libbz2.dll'," ^
      "    'Library\bin\libmp3lame.dll'," ^
      "    'Library\bin\libosmodsp-0.dll'," ^
      "    'Library\bin\FLAC.dll'," ^
      "    'Library\bin\SoapySDR.dll'," ^
      "    'Library\bin\airspy.dll'," ^
      "    'Library\bin\airspyhf.dll'," ^
      "    'Library\bin\bladeRF-2.dll'," ^
      "    'Library\bin\boost_filesystem.dll'," ^
      "    'Library\bin\boost_program_options.dll'," ^
      "    'Library\bin\boost_serialization.dll'," ^
      "    'Library\bin\boost_thread.dll'," ^
      "    'Library\bin\fftw3f.dll'," ^
      "    'Library\bin\fmt.dll'," ^
      "    'Library\bin\gnuradio-blocks.dll'," ^
      "    'Library\bin\gnuradio-fft.dll'," ^
      "    'Library\bin\gnuradio-filter.dll'," ^
      "    'Library\bin\gnuradio-iqbalance.dll'," ^
      "    'Library\bin\gnuradio-lora_sdr.dll'," ^
      "    'Library\bin\gnuradio-network.dll'," ^
      "    'Library\bin\gnuradio-osmosdr.dll'," ^
      "    'Library\bin\gnuradio-pdu.dll'," ^
      "    'Library\bin\gnuradio-pmt.dll'," ^
      "    'Library\bin\gnuradio-uhd.dll'," ^
      "    'Library\bin\hackrf-0.dll'," ^
      "    'Library\bin\libblas.dll'," ^
      "    'Library\bin\libcblas.dll'," ^
      "    'Library\bin\liblapack.dll'," ^
      "    'Library\bin\liblzma.dll'," ^
      "    'Library\bin\mirisdr-4.dll'," ^
      "    'Library\bin\mpg123.dll'," ^
      "    'Library\bin\mpir.dll'," ^
      "    'Library\bin\ogg.dll'," ^
      "    'Library\bin\opus.dll'," ^
      "    'Library\bin\rtlsdr.dll'," ^
      "    'Library\bin\sndfile.dll'," ^
      "    'Library\bin\spdlog.dll'," ^
      "    'Library\bin\volk.dll'," ^
      "    'Library\bin\vorbisenc.dll'," ^
      "    'Library\bin\uhd.dll'," ^
      "    'Library\bin\vorbis.dll'," ^
      "    'Library\bin\gnuradio-runtime.dll'," ^
      "    'Library\bin\libwinpthread-1.dll'," ^
      "    'Library\bin\libusb-1.0.dll'" ^
      ");" ^
      "$keepFull = $keepRelative | ForEach-Object { Join-Path $envRoot $_ };" ^
      "$dirs = $keepFull | ForEach-Object { Split-Path $_ -Parent } | Sort-Object -Unique;" ^
      "foreach ($d in $dirs) {" ^
      "  if (Test-Path $d) {" ^
      "    Get-ChildItem -Path $d -Filter '*.dll' -File |" ^
      "      Where-Object { $keepFull -notcontains $_.FullName } |" ^
      "      Remove-Item -Force" ^
      "  }" ^
      "}"
)

echo Bonus: Clear cache to free up space on your PC
echo Cleaning cache Micromamba...
micromamba clean --all --yes

echo Done, now you can use the internal radio of the app.
pause