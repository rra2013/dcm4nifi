package org.rra.cmove;

@FunctionalInterface
public interface IMoveComplete {
    void moveComplete(String studyIUID, String seriesIUID);
}
