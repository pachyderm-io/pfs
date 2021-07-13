for f in /pfs/scraper/*;
do
    cat $f <(html2text $f | tr -s '[[:punct:][:space:]]' '\n' | sort | uniq -c | sort -k1 -n) > /pfs/out/$(basename $f);
done
