#!/bin/bash
# Renombra .js a .gs después de clasp pull

for file in *.js; do
  if [[ "$file" != "appsscript.json" ]]; then
    base="${file%.js}"
    if [ -f "$base.gs" ]; then
      rm "$file"  # Borra el .js si ya existe el .gs
    else
      mv "$file" "$base.gs"
    fi
  fi
done

echo "✓ Archivos renombrados a .gs"