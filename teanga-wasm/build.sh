# build.sh
#!/bin/bash
echo "🦀 Building WASM package..."
wasm-pack build --target web --out-dir pkg --profile wasm-release
echo "✅ WASM build complete!"