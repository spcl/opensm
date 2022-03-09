rm -rf build
./autogen.sh
mkdir build
cd build
CFLAGS="-g -O0" ../configure
make -j
make install
