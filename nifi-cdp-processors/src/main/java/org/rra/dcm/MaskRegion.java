package org.rra.dcm;

import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class MaskRegion {
    int x;
    int y;
    int width;
    int height;

    public MaskRegion(int x, int y, int width, int height) {
        if (x < 0 || y < 0) {
            throw new IllegalArgumentException(
                    "x und y dürfen nicht negativ sein");
        }

        if (width <= 0 || height <= 0) {
            throw new IllegalArgumentException(
                    "width und height müssen größer als 0 sein");
        }

        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
    }
}
