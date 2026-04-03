package com.momao.valkey.example;

import com.momao.valkey.core.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/sku")
public class SkuController {

    private final SkuRepository skuRepository;

    private final SearchService searchService;

    public SkuController(SkuRepository skuRepository, SearchService searchService) {
        this.skuRepository = skuRepository;
        this.searchService = searchService;
    }

    @GetMapping("/info")
    public Map<String, Object> getInfo() {
        return Map.of(
                "connection", skuRepository.getConnectionInfo(),
                "indexName", SkuQuery.METADATA.indexName(),
                "prefix", SkuQuery.METADATA.prefix(),
                "prefixes", SkuQuery.METADATA.prefixes(),
                "storageType", SkuQuery.METADATA.storageType(),
                "fields", SkuQuery.METADATA.fields()
        );
    }

    @PostMapping
    public Sku createSku(@RequestBody Sku sku) {
        skuRepository.save(sku);
        return sku;
    }

    @GetMapping("/page")
    public Page<Sku> pageSkus(
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(required = false) String title) {
        SkuQuery q = new SkuQuery();
        if (title != null && !title.isBlank()) {
            return skuRepository.queryChain()
                    .where(q.title.contains(title))
                    .orderByAsc("price")
                    .page(offset, limit);
        }
        return skuRepository.queryChain()
                .orderByAsc("price")
                .page(offset, limit);
    }

    @GetMapping("/search/premium")
    public SearchService.SearchResponse searchPremium() {
        return searchService.searchPremiumPhones();
    }

    @GetMapping("/search")
    public SearchService.SearchResponse searchByMerchantAndPrice(
            @RequestParam String merchantName,
            @RequestParam Integer minPrice,
            @RequestParam Integer maxPrice) {
        return searchService.searchByMerchantAndPrice(merchantName, minPrice, maxPrice);
    }

    @GetMapping("/search/exclude")
    public SearchService.SearchResponse searchExcludingTag(@RequestParam String excludeTag) {
        return searchService.searchExcludingTag(excludeTag);
    }
}
