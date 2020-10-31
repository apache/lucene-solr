for file in dist/*; do
   
    #"$(basename "$file")"
    cksum = "$(sha512sum "$file")" |  jq --slurp --raw-input 'split("  ")[:-1]'
    echo cksum
done
