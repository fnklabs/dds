package com.fnklabs.dds;

import java.util.ArrayList;
import java.util.Collection;

public final class CollectionUtils {
    private CollectionUtils() {
    }

    /**
     * Get next node from specified items
     *
     * @param items   Items collection
     * @param current Current element in items
     *
     * @return Next node
     */
    public static <T> T getNext(Collection<T> items, T current) {

        ArrayList<T> collection = new ArrayList<>(items);


        int index = 0;
        for (int i = 0; i < collection.size(); i++) {
            T node = collection.get(i);
            if (node.equals(current)) {
                index = i;
                break;
            }
        }

        int nextMemberIndex = (index + 1) % items.size();

        return collection.get(nextMemberIndex);//members.get(nextMemberIndex);
    }
}
