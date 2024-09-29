@echo off
:: Check if Docker is installed
docker --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo Docker is not installed. Please install Docker and try again.
    exit /b 1
)

:: If Docker is installed, proceed with the build
echo Docker is installed. Proceeding with the build.

# get all submodules files
git submodule update --init

:: Customize the path to Dockerfile and image tag as needed
set DOCKERFILE_PATH=.\templates\containers\nosograph_assemblers\Dockerfile
set BUILD_DIR=.\templates\containers\nosograph_assemblers
set IMAGE_NAME=nosograph-assemblers

set OUTPUT_PATH=.\templates\containers\build
if not exist %OUTPUT_PATH% mkdir %OUTPUT_PATH%

docker build -t %IMAGE_NAME% -f %DOCKERFILE_PATH% %BUILD_DIR%

:: Check if the build succeeded
if %ERRORLEVEL% equ 0 (
    echo Docker image built successfully: %IMAGE_NAME%
    echo Saving image to file:  %OUTPUT_PATH%\%IMAGE_NAME%.tar
) else (
    echo Docker build failed.
    exit /b 1
)

docker save %IMAGE_NAME% -o %OUTPUT_PATH%\%IMAGE_NAME%.tar

set DIND_CONTAINER_NAME=dind-service
if not exist .\certs mkdir .\certs

docker compose build
docker compose up -d
docker cp %OUTPUT_PATH%\%IMAGE_NAME%.tar %DIND_CONTAINER_NAME%:/%IMAGE_NAME%.tar
docker exec %DIND_CONTAINER_NAME% docker load -i /%IMAGE_NAME%.tar
docker exec %DIND_CONTAINER_NAME% docker images
