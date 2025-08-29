# build-wasm.sh
#!/bin/bash
echo "🦀 Building WASM package..."
wasm-pack build --target web --out-dir pkg --release
echo "✅ WASM build complete!"