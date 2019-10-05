package com.BIO.getList;

import com.BIO.POJO.Product;

import java.util.List;

public interface iGetCSVListOfProduct {
    List<Product> processInputCSVFilteredByIDAndCount(String inputFilePath);

    List<Product> getProductsList();
}
