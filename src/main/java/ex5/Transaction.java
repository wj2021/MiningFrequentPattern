package ex5;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Transaction<T> {
    private List<T> items;

    public Transaction(List<T> items) {
        this.items = items;
    }

    public Transaction(String itemsStr, String sep) {
        if(itemsStr != null && !itemsStr.isEmpty() && sep != null && !sep.isEmpty()) {
            items = new ArrayList<T>();
            String[] tokens = itemsStr.split(sep);
            for (String token : tokens) {
                items.add((T) token);
            }
        }
    }


    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    public void distinct() {
        if(items != null && !items.isEmpty()) {
            Set<T> itemSet = new TreeSet<>(items);
            items = new ArrayList<>(itemSet);
        }
    }

    @Override
    public String toString() {
        return items.toString();
    }
}
