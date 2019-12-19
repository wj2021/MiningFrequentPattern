package ex5;

import java.util.*;

public class Apriori<T> {
    private List<Transaction<T>> transactionList;
    private Double minSupp;

    public Apriori() {
        transactionList = null;
        minSupp = 1.0;
    }

    public Apriori(List<Transaction<T>> transactionList, Double minSupp) {
        this.transactionList = transactionList;
        this.minSupp = minSupp;
    }

    public List<Transaction<T>> getTransactionList() {
        return transactionList;
    }

    public void setTransactionList(List<Transaction<T>> transactionList) {
        this.transactionList = transactionList;
    }

    public Double getMinSupp() {
        return minSupp;
    }

    public void setMinSupp(Double minSupp) {
        this.minSupp = minSupp;
    }

    public Map<Set<T>, Integer> getFrequentPattern() throws Exception {
        if(transactionList == null || transactionList.isEmpty()) {
            throw new Exception("transaction must be not empty");
        }

        if(minSupp < 0 || minSupp > 1) {
            throw new Exception("support must between 0 and 1");
        }

        final int transactionCount = transactionList.size();

        final int minCount = (int)java.lang.Math.ceil(transactionCount * minSupp); // 向上取整

        Map<T, Integer> itemCount = new HashMap<>();
        for(Transaction<T> transaction : transactionList) {
            transaction.distinct();
            List<T> items = transaction.getItems();
            for(T item : items) {
                if(itemCount.containsKey(item)) {
                    itemCount.put(item, itemCount.get(item)+1);
                } else {
                    itemCount.put(item, 1);
                }
            }
        }

        // 1项频繁集结果
        Map<Set<T>, Integer> result1 = new HashMap<>();
        for(Map.Entry<T, Integer> item : itemCount.entrySet()) {
            if(item.getValue() >= minCount) {
                Set<T> itemSet = new HashSet<>();
                itemSet.add(item.getKey());
                result1.put(itemSet, item.getValue());
            }
        }

        // 频繁项集结果，key为频繁项集，value为出现的次数
        Map<Set<T>, Integer> result = new HashMap<>(result1);

//        System.out.println("1项集结果=====================");
//        for(Map.Entry<Set<T>, Integer> item : result1.entrySet()) {
//            System.out.print(Arrays.toString(item.getKey().toArray()) + " " + item.getValue() + "/" + transactionCount);
//            System.out.print(" = ");
//            System.out.println(item.getValue()/(double)transactionCount);
//        }
//        System.out.println("=============================");

        int t = 1;
        Map<Set<T>,Integer> tempResult = result1;
        while(tempResult.size() > 1) {
            // 利用t项频繁集获得(t+1)项候选集
            t++; // 当前需要计算的项集的集合元素个数
            List<Set<T>> itemSetList = new ArrayList<>(tempResult.keySet()); // t项集
            List<Set<T>> itemSetList2 = new ArrayList<>(); // t+1项集
            int size = itemSetList.size();
            for(int i = 0; i < size; ++i) {
                for(int j = i+1; j < size; ++j) {
                    Set<T> newSet = new HashSet<>(itemSetList.get(i));
                    newSet.addAll(itemSetList.get(j)); // 集合的并运算
                    if(newSet.size() == t && !itemSetList2.contains(newSet)) {
                        itemSetList2.add(newSet);
                    }
                }
            }

            // (t+1)项集出现次数统计
            Map<Set<T>, Integer> itemCount2 = new HashMap<>();
            for(Set<T> itemSet : itemSetList2) {
                for(Transaction<T> transaction : transactionList) {
                    transaction.distinct();
                    List<T> items = transaction.getItems();
                    if(items.containsAll(itemSet)) {
                        if(itemCount2.containsKey(itemSet)) {
                            itemCount2.put(itemSet, itemCount2.get(itemSet)+1);
                        } else {
                            itemCount2.put(itemSet, 1);
                        }
                    }
                }
            }

            // (t+1)项集频繁结果
            Map<Set<T>, Integer> result2 = new HashMap<>();
            for (Map.Entry<Set<T>, Integer> item : itemCount2.entrySet()) {
                if(item.getValue() >= minCount) {
                    result2.put(item.getKey(), item.getValue());
                }
            }

            result.putAll(result2);

            tempResult = result2;

//            if(tempResult.size() > 0) {
//                System.out.println(t + "项集结果=====================");
//                for (Map.Entry<Set<T>, Integer> item : tempResult.entrySet()) {
//                    System.out.print(Arrays.toString(item.getKey().toArray()) + " " + item.getValue() + "/" + transactionCount);
//                    System.out.print(" = ");
//                    System.out.println(item.getValue() / (double) transactionCount);
//                }
//                System.out.println("================================");
//            }
        }

        return result;
    }
}
