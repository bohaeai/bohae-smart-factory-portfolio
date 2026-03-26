"use client";

import { useQuery } from "@tanstack/react-query";
import { twinApi } from "@/lib/api/services/digital-twin";
import type { PlantState, LineState, EquipmentState, SensorLatest, TwinEvent } from "@/lib/api/services/digital-twin";

export function usePlantState() {
  return useQuery({
    queryKey: ["twin", "plant"],
    queryFn: twinApi.getPlant,
    refetchInterval: 10_000,
    retry: false,
  });
}

export function useLineStates() {
  return useQuery({
    queryKey: ["twin", "lines"],
    queryFn: twinApi.getLines,
    refetchInterval: 10_000,
    retry: false,
    select: (d) => d.lines ?? [],
  });
}

export function useEquipmentStates() {
  return useQuery({
    queryKey: ["twin", "equipment"],
    queryFn: twinApi.getEquipment,
    refetchInterval: 30_000,
    retry: false,
    select: (d) => d.equipment ?? [],
  });
}

export function useSensorLatest() {
  return useQuery({
    queryKey: ["twin", "sensors"],
    queryFn: twinApi.getSensors,
    refetchInterval: 10_000,
    retry: false,
    select: (d) => d.sensors ?? [],
  });
}

export function useTwinEvents(limit = 20) {
  return useQuery({
    queryKey: ["twin", "events", limit],
    queryFn: () => twinApi.getEvents(limit),
    refetchInterval: 15_000,
    retry: false,
    select: (d) => d.events ?? [],
  });
}

export type { PlantState, LineState, EquipmentState, SensorLatest, TwinEvent };

