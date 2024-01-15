package com.example.dataanalysermicroservice.config;

import com.jcabi.xml.XML;
import lombok.RequiredArgsConstructor;

/**
 * Класс для обработки xml-файлов
 */
@RequiredArgsConstructor
public final class TextXPath {
    private final XML xml;
    /**
     * Та часть xml, в которой мы будем искать необходимые теги
     */
    private final String node;

    @Override
    public String toString() {
        return this.xml.nodes(this.node)// получаем список вложенных тегов в переданный тег 'node'
                .get(0)
                .xpath("text()")
                .get(0);
    }

}
