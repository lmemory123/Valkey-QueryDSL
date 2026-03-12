package com.momao.valkey.example;

import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.Page;
import jakarta.annotation.Resource;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SearchService {

    @Resource
    private SkuRepository skuRepository;

    public SearchResponse searchPremiumPhones() {
        SkuQuery q = new SkuQuery();

        SearchCondition condition = q.tags.contains("HOT")
                .and(q.price.between(1000, 5000))
                .and(q.title.contains("Pro"))
                .sortBy("price", true);

        Page<Sku> result = skuRepository.page(condition, 0, 20);

        System.out.println("[SearchService] query=" + condition.build());
        System.out.println("[SearchService] total=" + result.total());
        System.out.println("[SearchService] records=" + result.records());

        return new SearchResponse(condition.build(), result.total(), result.records());
    }

    public SearchResponse searchByMerchantAndPrice(String merchantName, Integer minPrice, Integer maxPrice) {
        SkuQuery q = new SkuQuery();

        SearchCondition condition = q.merchant.name.eq(merchantName)
                .and(q.price.between(minPrice, maxPrice))
                .sortBy("price", true);

        Page<Sku> result = skuRepository.page(condition, 0, 50);

        System.out.println("[SearchService] merchantPriceQuery=" + condition.build());

        return new SearchResponse(condition.build(), result.total(), result.records());
    }

    public SearchResponse searchExcludingTag(String excludeTag) {
        SkuQuery q = new SkuQuery();

        SearchCondition condition = q.tags.eq(excludeTag).not();

        Page<Sku> result = skuRepository.page(condition, 0, 50);

        System.out.println("[SearchService] excludeTagQuery=" + condition.build());

        return new SearchResponse(condition.build(), result.total(), result.records());
    }

    public record SearchResponse(
            String query,
            long total,
            List<Sku> records) {
    }
}
