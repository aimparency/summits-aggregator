# summits-aggregator

## preparations

Clone repo and `cd` into it. 

Add postgres db url to `./env`
```
DATABASE_URL=postgres://summits@localhost/summits 
```

Setup database with diesel: 

`diesel migrate run`

## run 
`cargo run`


